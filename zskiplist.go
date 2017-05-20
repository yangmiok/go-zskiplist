// This skiplist implementation is almost a translation of the original
// algorithm described by William Pugh in "Skip Lists: A Probabilistic
// Alternative to Balanced Trees", modified in three ways:
// a) this implementation allows for repeated scores.
// b) the comparison is not just by key (our 'score') but by satellite data.
// c) there is a back pointer, so it's a doubly linked list with the back
// pointers being only at "level 1". This allows to traverse the list
// from tail to head.
//
// https://github.com/antirez/redis/blob/unstable/src/t_zset.c

package zskiplist

import (
	"bytes"
	"fmt"
	"io"
)

const (
	ZSKIPLIST_MAXLEVEL = 12     // Should be enough for 2^32 elements
	ZSKIPLIST_P        = 250    // Skiplist P = 1/4, in thousandth
	RAND_MAX           = 0x7FFF //
)

type RankObjectInterface interface {
	IsGreaterThan(RankObjectInterface) bool
	IsEqualTo(RankObjectInterface) bool
}

// each level of list node
type zskipListLevel struct {
	forward *ZSkipListNode // link to next node
	span    int32          // node range across next
}

// list node
type ZSkipListNode struct {
	score    uint32
	level    []zskipListLevel
	backward *ZSkipListNode
	Obj      RankObjectInterface
}

func newZSkipListNode(level int, score uint32, obj RankObjectInterface) *ZSkipListNode {
	return &ZSkipListNode{
		score: score,
		Obj:   obj,
		level: make([]zskipListLevel, level),
	}
}

// Next return next forward pointer
func (n *ZSkipListNode) Next() *ZSkipListNode {
	return n.level[0].forward
}

// ZSkipList with descend order
type ZSkipList struct {
	head   *ZSkipListNode // header node
	tail   *ZSkipListNode // tail node
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
func (zsl *ZSkipList) Insert(score uint32, obj RankObjectInterface) *ZSkipListNode {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var rank [ZSKIPLIST_MAXLEVEL]int32

	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		// store rank that is crossed to reach the insert position
		if i != zsl.level-1 {
			rank[i] = rank[i+1]
		}
		for x.level[i].forward != nil {
			if x.level[i].forward.score > score ||
				(x.level[i].forward.score == score &&
					x.level[i].forward.Obj.IsGreaterThan(obj)) {
				rank[i] += x.level[i].span
				x = x.level[i].forward
			} else {
				break
			}
		}
		update[i] = x
	}
	// we assume the key is not already inside, since we allow duplicated
	// scores, and the re-insertion of score and redis object should never
	// happen since the caller of zslInsert() should test in the hash table
	// if the element is already inside or not.
	var level = zsl.randLevel()
	if level > zsl.level {
		for i := zsl.level; i < level; i++ {
			update[i] = zsl.head
			update[i].level[i].span = int32(zsl.length)
		}
		zsl.level = level
	}
	x = newZSkipListNode(level, score, obj)
	for i := 0; i < level; i++ {
		x.level[i].forward = update[i].level[i].forward
		update[i].level[i].forward = x

		//update span covered by update[i] as x is inserted here
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}
	//increment span for untouched levels
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

// findGreaterThan find an item greater than `obj`
func (zsl *ZSkipList) findGreaterThan(score uint32, obj RankObjectInterface, update []*ZSkipListNode) *ZSkipListNode {
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil {
			if x.level[i].forward.score > score ||
				(x.level[i].forward.score == score &&
					x.level[i].forward.Obj.IsGreaterThan(obj)) {
				x = x.level[i].forward
			} else {
				break
			}
		}
		if update != nil {
			update[i] = x
		}
	}
	if x != nil {
		return x.level[0].forward
	}
	return nil
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
func (zsl *ZSkipList) Delete(score uint32, obj RankObjectInterface) bool {
	var update [ZSKIPLIST_MAXLEVEL]*ZSkipListNode
	var x = zsl.findGreaterThan(score, obj, update[:])
	// We may have multiple elements with the same score, what we need
	// is to find the element with both the right score and object.
	if x != nil && score == x.score && x.Obj.IsEqualTo(obj) {
		zsl.deleteNode(x, update[0:])
		return true
	}
	return false // not found
}

// IsContains check whether `obj` is in this list
func (zsl *ZSkipList) IsContains(score uint32, obj RankObjectInterface) bool {
	var x = zsl.findGreaterThan(score, obj, nil)
	if x != nil && score == x.score && x.Obj.IsEqualTo(obj) {
		return true
	}
	return false
}

// GetRank Find the rank for an element by both score and key.
// Returns 0 when the element cannot be found, rank otherwise.
// Note that the rank is 1-based due to the span of zsl->header to the
// first element.
func (zsl *ZSkipList) GetRank(score uint32, obj RankObjectInterface) int32 {
	var rank int32 = 0
	var x = zsl.head
	for i := zsl.level - 1; i >= 0; i-- {
		for x.level[i].forward != nil {
			if x.level[i].forward.score > score ||
				(x.level[i].forward.score == score &&
					!obj.IsGreaterThan(x.level[i].forward.Obj)) { // greater than or equal to `obj`
				rank += x.level[i].span
				x = x.level[i].forward
			} else {
				break
			}
		}
		// x might be equal to zsl->header, so test if obj is non-nil
		if x.Obj != nil && x.Obj.IsEqualTo(obj) {
			return rank
		}
	}
	return 0
}

// GetElementByRank Finds an element by its rank.
// The rank argument needs to be 1-based.
func (zsl *ZSkipList) GetElementByRank(rank int32) *ZSkipListNode {
	var tranversed int32 = 0
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
func (zsl *ZSkipList) GetTopRankRange(n int) []*ZSkipListNode {
	var ranks = make([]*ZSkipListNode, 0, n)
	var x = zsl.head.level[0].forward
	for x != nil && n > 0 {
		ranks = append(ranks, x)
		n--
		x = x.level[0].forward
	}
	return ranks
}

// GetNearRange get range near to rank
func (zsl *ZSkipList) GetNearRange(rank int32, up, down int) []*ZSkipListNode {
	var target = zsl.GetElementByRank(rank)
	if target == nil {
		return nil
	}
	var ranks = make([]*ZSkipListNode, 0, up+down+1)
	var x = target.backward
	for x != nil && up > 0 {
		ranks = append(ranks, x)
		up--
		x = x.backward
	}
	ranks = append(ranks, target)
	x = target.level[0].forward
	for x != nil && down > 0 {
		ranks = append(ranks, x)
		down--
		x = x.level[0].forward
	}
	return ranks
}

// Walk iterate list by `fn` with max `loop`
func (zsl *ZSkipList) Walk(loop int, fn func(int, RankObjectInterface)) {
	var rank = 1
	var node = zsl.head.level[0].forward
	for node != nil && loop > 0 {
		fn(rank, node.Obj)
		rank++
		loop--
		node = node.level[0].forward
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
	n, _ := fmt.Fprintf(w, "<  head> ")
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

func (zsl *ZSkipList) dumpNode(w io.Writer, node *ZSkipListNode) {
	var line bytes.Buffer
	n, _ := fmt.Fprintf(w, "<%6d> ", node.score)
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
	var tranversed int32 = 0
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
