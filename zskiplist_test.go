// +build !ignore

package zskiplist

import (
	"math/rand"
	"os"
	"testing"
)

var _ = os.Open

const testRandSeed = 123456789

func init() {
	rand.Seed(testRandSeed)
}

type testPlayer struct {
	uid      uint64
	Populace uint32
	Level    uint16
}

// populace is already equal
func (p *testPlayer) IsGreaterThan(b RankInterface) bool {
	var rhs = b.(*testPlayer)
	if p.Populace == rhs.Populace {
		if p.Level == rhs.Level {
			return p.uid < rhs.uid
		}
		return p.Level > rhs.Level
	}
	return p.Populace > rhs.Populace
}

func (p *testPlayer) Uuid() uint64 {
	return p.uid
}

func rangePerm(min, max int) []int {
	if min > max {
		panic("RangePerm: min greater than max")
	}
	if min == max {
		return []int{min}
	}
	list := rand.Perm(max - min + 1)
	for i := 0; i < len(list); i++ {
		list[i] += min
	}
	return list
}

func makeTestData(count, score int) map[uint64]*testPlayer {
	var set = make(map[uint64]*testPlayer, count)
	var startID uint64 = 1234567890
	for i := 0; i < count; i++ {
		startID++
		obj := &testPlayer{
			Level:    uint16(rand.Int() % 60),
			Populace: uint32(rand.Int() % count), // can be duplicated
		}
		obj.uid = startID
		set[obj.uid] = obj
	}
	return set
}

// make a skip list with sequential sorder scores, score equal to id
func makeTestSkipList(count int, set map[uint64]*testPlayer) *ZSkipList {
	var zsl = NewZSkipList(testRandSeed)
	var list = rangePerm(1, count)
	for _, n := range list {
		obj := &testPlayer{
			Level:    uint16(rand.Int() % 60),
			Populace: uint32(n),
			uid:      uint64(n),
		}
		set[obj.uid] = obj
		if zsl.Insert(obj.Populace, obj) == nil {
			panic("insert failed")
		}
	}
	return zsl
}

func dumpToFile(zsl *ZSkipList, filename string) {
	f, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	zsl.Dump(f)
}

func TestZSkipListInsert(t *testing.T) {
	const units = 500000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert((v.Populace), v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	if zsl.Len() != units {
		t.Fatalf("skiplist elements count not equal")
	}
	t.Logf("skiplist %d item with height %d", units, zsl.Height())
	for _, v := range set {
		var node = zsl.Delete((v.Populace), v)
		if node == nil {
			t.Fatalf("delete item[%d-%d] failed", v.Populace, v.uid)
		}
		if brief := node.Obj.(*testPlayer); brief.uid != v.uid {
			t.Fatalf("delete item, %d not equal to %d", brief.uid, v.uid)
		}
	}
	if zsl.Len() != 0 {
		t.Fatalf("skiplist not empty")
	}
}

func TestZSkipListChangeInsert(t *testing.T) {
	const units = 500000
	var set = makeTestData(units, 100)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert((v.Populace), v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	var maxCount = units / 2
	for _, v := range set {
		var oldScore = v.Populace
		var node = zsl.Delete(oldScore, v)
		if node == nil {
			//dumpToFile(zsl, "zskiplist-ci.dat")
			t.Fatalf("delete dup item[%d-%d] fail", v.uid, v.Populace)
			break
		}
		if brief := node.Obj.(*testPlayer); brief.uid != v.uid {
			//dumpToFile(zsl, "zskiplist-ci.dat")
			t.Fatalf("delete dup item, %d not equal to %d", brief.uid, v.uid)
		}

		v.Populace += uint32(rand.Int() % 100)
		if node := zsl.Insert(v.Populace, v); node == nil {
			//dumpToFile(zsl, "zskiplist-ci.dat")
			t.Fatalf("insert dup item %d fail, %d/%d", v.uid, oldScore, v.Populace)
			break
		}
		maxCount--
		if maxCount == 0 {
			break
		}
	}
	for _, v := range set {
		var node = zsl.Delete((v.Populace), v)
		if node == nil {
			t.Fatalf("delete set item[%d-%d] failed", v.Populace, v.uid)
		}
		if brief := node.Obj.(*testPlayer); brief.uid != v.uid {
			t.Fatalf("delete set item, %d not equal to %d", brief.uid, v.uid)
		}
	}
	if zsl.Len() != 0 {
		t.Fatalf("skiplist not empty")
	}
}

func TestZSkipListGetRank(t *testing.T) {
	const units = 500000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	//dumpToFile(zsl, "zskiplist-rank.dat")
	var expectRank = int32(1)
	for i := units; i > 0; i-- {
		var obj = set[uint64(i)]
		var rank = zsl.GetRank(obj.Populace, obj)
		if rank == 0 {
			t.Fatalf("get rank[%d-%d] failed", obj.Populace, obj.uid)
		}
		if rank != expectRank {
			t.Fatalf("item[%d-%d] expect rank %d, got %d", obj.Populace, obj.uid, expectRank, rank)
		}
		expectRank++
	}
}

func TestZSkipListElementByRank(t *testing.T) {
	const units = 500000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert((v.Populace), v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	for _, v := range set {
		var rank = zsl.GetRank((v.Populace), v)
		if rank <= 0 {
			t.Fatalf("get rank of item[%d-%d] failed", v.Populace, v.uid)
		}
		var node = zsl.GetElementByRank(rank)
		if node == nil {
			t.Fatalf("get object by rank[%d] failed", rank)
		}
		if node.Obj != v {
			t.Fatalf("rank[%d] object[%d-%d] not equal", rank, v.Populace, v.uid)
		}
	}
}

func BenchmarkZSkipListInsert(b *testing.B) {
	b.StopTimer()
	var zsl = NewZSkipList(testRandSeed)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		obj := &testPlayer{
			Level:    uint16(i),
			Populace: uint32(i),
		}
		obj.uid = uint64(i)
		if node := zsl.Insert((obj.Populace), obj); node == nil {
			b.Fatalf("insert item[%d-%d] failed", obj.Populace, obj.uid)
		}
	}
}

func BenchmarkZSkipListRemove(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		var obj = set[uint64(i)]
		var node = zsl.Delete(obj.Populace, obj)
		if node == nil {
			b.Fatalf("delete item[%d-%d] fail", obj.Populace, obj.uid)
		}
		if i <= units {
			b.Fatalf("delete item[%d-%d] failed", obj.Populace, obj.uid)
		}
	}
}

func BenchmarkZSkipListGetRank(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		var obj = set[uint64(rand.Int()%units)+1]
		if obj != nil {
			var rank = zsl.GetRank(obj.Populace, obj)
			if rank <= 0 && i <= units {
				b.Fatalf("get rank of item[%d-%d] failed", obj.uid, obj.Populace)
			}
		}
	}
}

func BenchmarkZSkipListGetElementByRank(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var rank = int32(rand.Int() % zsl.Len())
		zsl.GetElementByRank(rank)
	}
}
