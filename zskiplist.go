// This skiplist implementation is almost a translation of the original
// algorithm described by William Pugh in "Skip Lists: A Probabilistic
// Alternative to Balanced Trees", modified in three ways:
// a) this implementation allows for repeated scores.
// b) the comparison is not just by key (our 'score') but by satellite data.
// c) there is a back pointer, so it's a doubly linked list with the back
// pointers being only at "level 1". This allows to traverse the list
// from tail to head.
//
// https://github.com/antirez/redis/blob/3.2/src/t_zset.c

package zskiplist

import (
	"bytes"
	"fmt"
	"io"
)

const (
	ZSKIPLIST_MAXLEVEL = 12     // Should be enough
	ZSKIPLIST_P        = 250    // Skiplist P = 1/4, in thousandth
	RAND_MAX           = 0x7FFF //
)

//A type that satisfies RankInterface can be ranked in a zskiplist
type RankInterface interface {

	// CompareTo compare two rankable objects.
	// Return value:
	//    positive if s1 > s2.
	//    negative if s1 < s2.
	//    0 if s1 and s2 are exactly the same binary string.
	CompareTo(RankInterface) int
}

// each level of list node
type zskipListLevel struct {
	forward *ZSkipListNode // link to next node
	span    int            // node range across next
}

// list node
type ZSkipListNode struct {
	level    []zskipListLevel
	backward *ZSkipListNode
	Score    uint32
	Obj      RankInterface
}

func newZSkipListNode(level int, score uint32, obj RankInterface) *ZSkipListNode {
	return &ZSkipListNode{
		Score: score,
		Obj:   obj,
		level: make([]zskipListLevel, level),
	}
}

// Next return next forward pointer
func (n *ZSkipListNode) Next() *ZSkipListNode {
	return n.level[0].forward
}

// ZSkipList with ascend order
type ZSkipList struct {
	head   *ZSkipListNode // header node
	tail   *ZSkipListNode // tail node, this means the least item
	seed   uint64         // random number generator seed
	length int            // count of items
	level  int            //
}

func NewZSkipList(seed int64) *ZSkipList {
	return &ZSkipList{
		level: 1,
		seed:  uint64(seed),
		head:  newZSkipListNode(ZSKIPLIST_MAXLEVEL, 0, nil),
	}
}

// a simple linear congruential random number generator
func (zsl *ZSkipList) randNext() uint32 {
	zsl.seed = zsl.seed*214013 + 2531011
	return uint32(zsl.seed>>16) & RAND_MAX
}

// Returns a random level for the new skiplist node we are going to create.
// The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
// (both inclusive), with a powerlaw-alike distribution where higher
// levels are less likely to be returned.
func (zsl *ZSkipList) randLevel() int {
	var level = 1
	for level < ZSKIPLIST_MAXLEVEL && zsl.randNext() < uint32(RAND_MAX*ZSKIPLIST_P/1000) {
		level++
	}
	return level
}

// Len return # of items in list
func (zsl *ZSkipList) Len() int {
	return zsl.length
}

// Height return current level of list
func (zsl *ZSkipList) Height() int {
	return zsl.level
}

// HeadNode return the node after head
func (zsl *ZSkipList) HeadNode() *ZSkipListNode {
	return zsl.head.level[0].forward
}

// TailNode return the tail node
func (zsl *ZSkipList) TailNode() *ZSkipListNode {
	return zsl.tail
}

// Insert insert an object to skiplist with score
func (zsl *ZSkipList) Insert(score uint32, obj RankInterface) *ZSkipListNode {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var rank [ZSKIPLIST_MAXLEVEL]int

	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		// store rank that is crossed to reach the insert position
		if i != zsl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Obj.CompareTo(obj) < 0)) {
			rank[i] += x.level[i].span
			x = x.level[i].forward
		}
		update[i] = x
	}
	// we assume the key is not already inside, since we allow duplicated
	// scores, and the re-insertion of score and redis object should never
	// happen since the caller should test in the hash table  if the element
	// is already inside or not.
	var level = zsl.randLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			update[i] = zsl.head
			update[i].level[i].span = zsl.length
		}
		zsl.level = level
	}
	x = newZSkipListNode(level, score, obj)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		// update span covered by update[i] as x is inserted here
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	// increment span for untouched levels
	for i := level; i < zsl.level; i++ {
		update[i].level[i].span++
	}
	if update[0] != zsl.head {
		x.backward = update[0]
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x
	} else {
		zsl.tail = x
	}
	zsl.length++
	return x
}

func (zsl *ZSkipList) deleteNode(x *ZSkipListNode, update []*ZSkipListNode) {
	for i := 0; i < zsl.level; i++ {
		if update[i].level[i].forward == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].forward = x.level[i].forward
		} else {
			update[i].level[i].span -= 1
		}
	}
	if x.level[0].forward != nil {
		x.level[0].forward.backward = x.backward
	} else {
		zsl.tail = x.backward
	}
	for zsl.level > 1 && zsl.head.level[zsl.level-1].forward == nil {
		zsl.level--
	}
	zsl.length--
}

// Delete delete an element with matching score/object from the skiplist
func (zsl *ZSkipList) Delete(score uint32, obj RankInterface) *ZSkipListNode {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Obj.CompareTo(obj) < 0)) {
			x = x.level[i].forward
		}
		update[i] = x
	}

	// We may have multiple elements with the same score, what we need
	// is to find the element with both the right score and object.
	x = x.level[0].forward
	if x != nil && score == x.Score && x.Obj.CompareTo(obj) == 0 {
		zsl.deleteNode(x, update[0:])
		return x
	}
	return nil // not found
}

// Returns if there is a part of the zset is in range.
func (zsl *ZSkipList) IsInRange(min, max uint32) bool {
	// Test for ranges that will always be empty.
	if min > max {
		return false
	}
	var x = zsl.tail // maximum
	if x == nil || x.Score < min {
		return false
	}
	x = zsl.head.level[0].forward // minimum
	if x == nil || x.Score > max {
		return false
	}
	return true
}

// Find the first node that is contained in the specified range.
// Returns nil when no element is contained in the range.
func (zsl *ZSkipList) FirstInRange(min, max uint32) *ZSkipListNode {
	// If everything is out of range, return early.
	if !zsl.IsInRange(min, max) {
		return nil
	}
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		// Go foward while out of range
		for x.level[i].forward != nil && x.level[i].forward.Score < min {
			x = x.level[i].forward
		}
	}

	//This is an inner range, so the next node cannot be NULL.
	x = x.level[0].forward

	// Check is score <= max
	if x.Score > max {
		return nil
	}
	return x
}

// Find the last node that is contained in the specified range.
// Returns nil when no element is contained in the range.
func (zsl *ZSkipList) LastInRange(min, max uint32) *ZSkipListNode {
	// If everything is out of range, return early.
	if !zsl.IsInRange(min, max) {
		return nil
	}
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		// Go forward while in range
		for x.level[i].forward != nil && x.level[i].forward.Score <= max {
			x = x.level[i].forward
		}
	}

	// Check if score <= max
	if x.Score > max {
		return nil
	}
	return x
}

// Delete all the elements with score between [max, min] from the skiplist.
// Min and max are inclusive, so a score >= min || score <= max is deleted.
func (zsl *ZSkipList) DeleteRangeByScore(min, max uint32) uint32 {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && x.level[i].forward.Score <= min {
			x = x.level[i].forward
		}
		update[i] = x
	}

	//Current node is the last with score < or <= min
	x = x.level[0].forward

	// Delete nodes while in range
	var removed uint32
	for x != nil && x.Score <= max {
		var next = x.level[0].forward
		zsl.deleteNode(x, update[0:])
		removed++
		x = next
	}
	return removed
}

// Delete all the elements with rank between start and end from the skiplist.
// Start and end are inclusive. Note that start and end need to be 1-based
func (zsl *ZSkipList) DeleteRangeByRank(start, end int) uint32 {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var tranversed, removed uint32
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (int(tranversed)+x.level[i].span < start) {
			tranversed += uint32(x.level[i].span)
			x = x.level[i].forward
		}
		update[i] = x
	}
	tranversed++
	x = x.level[0].forward
	for x != nil && int(tranversed) <= end {
		var next = x.level[0].forward
		zsl.deleteNode(x, update[0:])
		removed++
		tranversed++
		x = next
	}
	return removed
}

// GetRank Find the rank for an element by both score and key.
// Returns 0 when the element cannot be found, rank otherwise.
// Note that the rank is 1-based due to the span of zsl->header to the first element.
func (zsl *ZSkipList) GetRank(score uint32, obj RankInterface) int {
	var rank = 0
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil &&
			(x.level[i].forward.Score < score ||
				(x.level[i].forward.Score == score &&
					x.level[i].forward.Obj.CompareTo(obj) <= 0)) {
			rank += x.level[i].span
			x = x.level[i].forward
		}

		// x might be equal to zsl->header, so test if obj is non-nil
		if x.Obj != nil && x.Obj.CompareTo(obj) == 0 {
			return rank
		}
	}
	return 0
}

// GetElementByRank Finds an element by its rank.
// The rank argument needs to be 1-based.
func (zsl *ZSkipList) GetElementByRank(rank int) *ZSkipListNode {
	var tranversed int = 0
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil && (tranversed+x.level[i].span <= rank) {
			tranversed += x.level[i].span
			x = x.level[i].forward
		}
		if tranversed == rank {
			return x
		}
	}
	return nil
}

// GetTopRankRange get top score of N elements
func (zsl *ZSkipList) GetTopRankValueRange(n int) []RankInterface {
	var ranks = make([]RankInterface, 0, n)
	var x = zsl.tail
	for x != nil && n > 0 {
		ranks = append(ranks, x.Obj)
		n--
		x = x.backward
	}
	return ranks
}

// GetNearRange get range near to rank
func (zsl *ZSkipList) GetNearValueRange(rank, up, down int) []RankInterface {
	var target = zsl.GetElementByRank(rank)
	if target == nil {
		return nil
	}
	var ranks = make([]RankInterface, 0, up+down+1)
	var x = target.backward
	for x != nil && up > 0 {
		ranks = append(ranks, x.Obj)
		up--
		x = x.backward
	}
	ranks = append(ranks, target.Obj)
	x = target.level[0].forward
	for x != nil && down > 0 {
		ranks = append(ranks, x.Obj)
		down--
		x = x.level[0].forward
	}
	return ranks
}

// Walk iterate list by `fn` with max `loop`
func (zsl *ZSkipList) Walk(startTail bool, fn func(int, RankInterface) bool) {
	if startTail { // from tail to head
		var rank = 1
		var node = zsl.tail
		for node != nil {
			if !fn(rank, node.Obj) {
				break
			}
			node = node.backward
			rank++
		}
	} else { // from head to tail
		var rank = zsl.length
		var node = zsl.head.level[0].forward
		for node != nil {
			if !fn(rank, node.Obj) {
				break
			}
			rank--
			node = node.level[0].forward
		}
	}
}

func (zsl ZSkipList) String() string {
	var buf bytes.Buffer
	zsl.Dump(&buf)
	return buf.String()
}

// Dump dump whole list to w, mostly for debug usage
func (zsl *ZSkipList) Dump(w io.Writer) {
	var x = zsl.head
	// dump header
	var line bytes.Buffer
	n, _ := fmt.Fprintf(w, "<             head> ")
	prePadding(&line, n)
	for i := 0; i < zsl.level; i++ {
		if i < len(x.level) {
			if x.level[i].forward != nil {
				fmt.Fprintf(w, "[%2d] ", x.level[i].span)
				line.WriteString("  |  ")
			}
		}
	}
	fmt.Fprint(w, "\n")
	line.WriteByte('\n')
	line.WriteTo(w)

	// dump list
	x = x.level[0].forward
	for x != nil {
		zsl.dumpNode(w, x)
		if len(x.level) > 0 {
			x = x.level[0].forward
		}
	}

	// dump tail end
	fmt.Fprintf(w, "<   end> ")
	for i := 0; i < zsl.level; i++ {
		fmt.Fprintf(w, "  _  ")
	}
	fmt.Fprintf(w, "\n")
}

type uider interface {
	Uid() uint64
}

func (zsl *ZSkipList) dumpNode(w io.Writer, node *ZSkipListNode) {
	var line bytes.Buffer
	var uuid string
	if u, ok := node.Obj.(uider); ok {
		uuid = fmt.Sprintf("%s", u.Uid())
	} else {
		uuid = fmt.Sprintf("%p", node.Obj)
	}
	n, _ := fmt.Fprintf(w, "<%s %6d> ", uuid, node.Score)
	prePadding(&line, n)
	for i := 0; i < zsl.level; i++ {
		if i < len(node.level) {
			fmt.Fprintf(w, "[%2d] ", node.level[i].span)
			line.WriteString("  |  ")
		} else {
			if shouldLinkVertical(zsl.head, node, i) {
				fmt.Fprintf(w, "  |  ")
				line.WriteString("  |  ")
			}
		}
	}
	fmt.Fprint(w, "\n")
	line.WriteByte('\n')
	line.WriteTo(w)
}

func shouldLinkVertical(head, node *ZSkipListNode, level int) bool {
	if node.backward == nil { // first element
		return head.level[level].span >= 1
	}
	var tranversed = 0
	var prev *ZSkipListNode
	var x = node.backward
	for x != nil {
		if level >= len(x.level) {
			return true
		}
		if x.level[level].span > tranversed {
			return true
		}
		tranversed++
		prev = x
		x = x.backward
	}
	if prev != nil && level < len(prev.level) {
		return prev.level[level].span >= tranversed
	}
	return false
}

func prePadding(line *bytes.Buffer, n int) {
	for i := 0; i < n; i++ {
		line.WriteByte(' ')
	}
}
