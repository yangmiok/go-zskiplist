// +build !ignore

package zskiplist

import (
	"math/rand"
	"testing"
)

const testRandSeed = 123456789

func init() {
	rand.Seed(testRandSeed)
}

type testPlayer struct {
	uid      uint64
	Populace uint32
	Level    uint16
}

func (p *testPlayer) IsGreaterThan(b RankObjectInterface) bool {
	var rhs = b.(*testPlayer)
	if p.Populace == rhs.Populace {
		if p.Level == rhs.Level {
			return p.uid < rhs.uid
		}
		return p.Level > rhs.Level
	}
	return p.Populace > rhs.Populace
}

func (p *testPlayer) IsEqualTo(b RankObjectInterface) bool {
	var rhs = b.(*testPlayer)
	return p.uid == rhs.uid
}

func makeTestData(count int) map[uint64]*testPlayer {
	var set = make(map[uint64]*testPlayer, count)
	var startID uint64 = 1234567890
	for i := 0; i < count; i++ {
		startID++
		obj := &testPlayer{
			Level:    uint16(rand.Int() % 60),
			Populace: uint32(rand.Int() % 1000), // can be duplicated
		}
		obj.uid = startID
		set[obj.uid] = obj
	}
	return set
}

// make a skip list with sequential sorder scores
func makeTestSkipList(count int, set map[uint64]*testPlayer) *ZSkipList {
	var zsl = NewZSkipList(testRandSeed)
	for i := count; i > 0; i-- {
		obj := &testPlayer{
			Level:    uint16(i - 1),
			Populace: uint32(i),
		}
		obj.uid = uint64(i)
		set[obj.uid] = obj
		zsl.Insert(obj.Populace, obj)
	}
	return zsl
}

func TestZSkipListInsert(t *testing.T) {
	const units = 100000
	var set = makeTestData(units)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert((v.Populace), v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	if zsl.Len() != units {
		t.Fatalf("size not equal")
	}
	t.Logf("skiplist %d item with height %d", units, zsl.Height())
	for _, v := range set {
		var ok = zsl.Delete((v.Populace), v)
		if !ok {
			t.Fatalf("delete item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	if zsl.Len() != 0 {
		t.Fatalf("size not equal")
	}
}

func TestZSkipListGetRank(t *testing.T) {
	const units = 100000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	//zsl.Dump(os.Stdout)
	var expectRank = int32(1)
	for i := units; i > 0; i-- {
		var obj = set[uint64(i)]
		var rank = zsl.GetRank(obj.Populace, obj)
		if rank <= 0 {
			t.Fatalf("get rank[%d-%d] failed", obj.Populace, obj.uid)
		}
		if rank != expectRank {
			t.Fatalf("item[%d-%d] expect rank %d, got %d", obj.Populace, obj.uid, expectRank, rank)
		}
		expectRank++
	}
}

func TestZSkipListElementByRank(t *testing.T) {
	const units = 100000
	var set = makeTestData(units)
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
		zsl.Insert((obj.Populace), obj)
	}
}

func BenchmarkZSkipListRemove(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var ok = zsl.Delete(uint32(i), set[uint64(i)])
		if !ok && i > 0 && i <= units {
			b.Fatalf("delete item[%d] failed", i)
		}
	}
}

func BenchmarkZSkipListGetRank(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = make(map[uint64]*testPlayer, units)
	var zsl = makeTestSkipList(units, set)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var score = uint32(rand.Int() % units)
		var rank = zsl.GetRank(score, set[uint64(score)])
		if rank <= 0 && i > 0 && i <= units {
			b.Fatalf("get rank of item[%d] failed")
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
		var node = zsl.GetElementByRank(rank)
		if node == nil {
			b.Fatalf("get element by rank[%d] failed", rank)
		}
	}
}
