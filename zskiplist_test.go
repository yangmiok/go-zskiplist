// +build !ignore

package zskiplist

import (
	"math/rand"
	"os"
	"sort"
	"testing"
	"time"
)

var _ = os.Open

var testRandSeed = time.Now().Unix()

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

func checkDuplicateObject(zsl *ZSkipList, t *testing.T) {
	if zsl.Len() == 0 {
		return
	}
	var set = make(map[uint64]bool, zsl.Len())
	zsl.Walk(func(rank int, obj RankInterface) bool {
		if _, found := set[obj.Uuid()]; found {
			var brief = obj.(*testPlayer)
			t.Fatalf("Duplicate rank object found: %d, %d", rank, brief.Uuid())
		}
		set[obj.Uuid()] = true
		return true
	})
}

func dumpToFile(zsl *ZSkipList, filename string) {
	f, _ := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	zsl.Dump(f)
}

func mapToSlice(set map[uint64]*testPlayer) []*testPlayer {
	var slice = make([]*testPlayer, 0, len(set))
	for _, v := range set {
		slice = append(slice, v)
	}
	return slice
}

func TestZSkipListInsertRemove(t *testing.T) {
	const units = 50000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)

	for i := 0; i < 100; i++ {
		for _, v := range set {
			if node := zsl.Insert(v.Populace, v); node == nil {
				t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
			}
		}
		if zsl.Len() != units {
			t.Fatalf("unexpected skiplist element count, %d != %d", zsl.Len(), units)
		}

		checkDuplicateObject(zsl, t)

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
}

func TestZSkipListChangedInsert(t *testing.T) {
	const units = 50000
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
	if zsl.Len() != units {
		t.Fatalf("unexpected skiplist element count")
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
	const units = 20000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}

	for i := 0; i < 100; i++ {
		var count = units / 2 // change half of the list
		for _, v := range set {
			var oldScore = v.Populace
			if node := zsl.Delete(oldScore, v); node == nil {
				t.Fatalf("delete rank item[%d-%d] fail", v.uid, v.Populace)
				break
			}
			v.Populace += uint32(rand.Int31() % 100)
			if node := zsl.Insert(v.Populace, v); node == nil {
				t.Fatalf("insert rank item[%d-%d] fail", v.uid, v.Populace)
				break
			}
			count--
			if count == 0 {
				break
			}
		}

		// rank by sort package
		var ranks = mapToSlice(set)
		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].IsGreaterThan(ranks[j])
		})

		for i, v := range ranks {
			var rank = zsl.GetRank(v.Populace, v)
			if rank != int32(i+1) {
				t.Fatalf("%d not equal at rank %d", v.Uuid(), i+1)
				break
			}
		}
	}
}

func TestZSkipListElementByRank(t *testing.T) {
	const units = 20000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			t.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	for i := 0; i < 100; i++ {
		var count = units / 2 // change half of the list
		for _, v := range set {
			var oldScore = v.Populace
			if node := zsl.Delete(oldScore, v); node == nil {
				t.Fatalf("delete rank item[%d-%d] fail", v.uid, v.Populace)
				break
			}
			v.Populace += uint32(rand.Int31() % 100)
			if node := zsl.Insert(v.Populace, v); node == nil {
				t.Fatalf("insert rank item[%d-%d] fail", v.uid, v.Populace)
				break
			}
			count--
			if count == 0 {
				break
			}
		}

		// rank by sort package
		var ranks = mapToSlice(set)
		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].IsGreaterThan(ranks[j])
		})

		for i, v := range ranks {
			var rank = int32(i + 1)
			var node = zsl.GetElementByRank(rank)
			if node == nil {
				t.Fatalf("get object by rank[%d] failed", rank)
			}
			if node.Obj.Uuid() != v.Uuid() {
				t.Fatalf("rank[%d] object[%d-%d] not equal", rank, v.Populace, v.uid)
			}
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
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			b.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		var obj *testPlayer
		for _, v := range set {
			obj = v
			break
		}
		zsl.Delete(obj.Populace, obj)
	}
}

func BenchmarkZSkipListGetRank(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			b.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	b.StartTimer()
	for i := 1; i < b.N; i++ {
		for _, v := range set {
			zsl.GetRank(v.Populace, v)
		}
	}
}

func BenchmarkZSkipListGetElementByRank(b *testing.B) {
	b.StopTimer()
	const units = 1000000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			b.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	// rank by sort package
	var ranks = mapToSlice(set)
	sort.Slice(ranks, func(i, j int) bool {
		return ranks[i].IsGreaterThan(ranks[j])
	})
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for i, _ := range ranks {
			zsl.GetElementByRank(int32(i + 1))
		}
	}
}
