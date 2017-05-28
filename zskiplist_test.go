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

func (p *testPlayer) Uid() uint64 {
	return p.uid
}

func (p *testPlayer) CompareTo(rhs RankInterface) int {
	var b = rhs.(*testPlayer)
	if p.uid == b.uid {
		return 0
	}
	switch {
	case p.Populace < b.Populace:
		return -1
	case p.Populace > b.Populace:
		return 1
	default:
		switch {
		case p.Level < b.Level:
			return -1
		case p.Level > b.Level:
			return 1
		default:
			switch {
			case p.uid < b.uid:
				return 1
			case p.uid > b.uid:
				return -1
			default:
				return 0
			}
		}
	}
}

func makeTestData(count, score int) map[uint64]*testPlayer {
	var set = make(map[uint64]*testPlayer, count)
	var startID uint64 = 1234567890
	for i := 0; i < count; i++ {
		obj := &testPlayer{
			uid:      startID,
			Level:    uint16(rand.Int() % 60),
			Populace: uint32(rand.Int() % count), // can be duplicated
		}
		set[obj.uid] = obj
		startID++
	}
	return set
}

func checkDuplicateObject(zsl *ZSkipList, t *testing.T) {
	if zsl.Len() == 0 {
		return
	}
	var set = make(map[uint64]bool, zsl.Len())
	zsl.Walk(true, func(rank int, obj RankInterface) bool {
		var brief = obj.(*testPlayer)
		if _, found := set[brief.uid]; found {
			t.Fatalf("Duplicate rank object found: %d, %d", rank, brief.uid)
		}
		set[brief.uid] = true
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

func manyUpdate(t *testing.T, zsl *ZSkipList, set map[uint64]*testPlayer, count int) {
	for _, v := range set {
		var oldScore = v.Populace
		if node := zsl.Delete(oldScore, v); node == nil {
			dumpToFile(zsl, "zskiplist.dat")
			t.Fatalf("delete item[%d-%d] fail", v.uid, v.Populace)
			break
		}
		v.Populace += uint32(rand.Int31() % 100)
		if node := zsl.Insert(v.Populace, v); node == nil {
			t.Fatalf("insert item[%d-%d] fail", v.uid, v.Populace)
			break
		}
		count--
		if count == 0 {
			break
		}
	}
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
	manyUpdate(t, zsl, set, units/2)
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
	// rank by sort package
	var ranks = mapToSlice(set)
	sort.Slice(ranks, func(i, j int) bool {
		return ranks[i].CompareTo(ranks[j]) < 0
	})
	for i := len(ranks); i > 0; i-- {
		var v = ranks[i-1]
		var rank = zsl.GetRank(v.Populace, v)
		if rank != i {
			t.Fatalf("%d not equal at rank %d", v.uid, i)
			break
		}
	}
}

func TestZSkipListUpdateGetRank(t *testing.T) {
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
		manyUpdate(t, zsl, set, units/2)

		// rank by sort package
		var ranks = mapToSlice(set)
		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].CompareTo(ranks[j]) < 0
		})
		for i := len(ranks); i > 0; i-- {
			var v = ranks[i-1]
			var rank = zsl.GetRank(v.Populace, v)
			if rank != i {
				t.Fatalf("%d not equal at rank %d", v.uid, i)
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
		manyUpdate(t, zsl, set, units/2)

		// rank by sort package
		var ranks = mapToSlice(set)
		sort.Slice(ranks, func(i, j int) bool {
			return ranks[i].CompareTo(ranks[j]) < 0
		})

		for i := len(ranks); i > 0; i-- {
			var v = ranks[i-1]
			var node = zsl.GetElementByRank(i)
			if node == nil {
				t.Fatalf("get object by rank[%d] failed", i)
			}
			var brief = node.Obj.(*testPlayer)
			if brief.uid != v.uid {
				t.Fatalf("rank[%d] object[%d-%d] not equal", i, v.Populace, v.uid)
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
			uid:      uint64(i),
			Level:    uint16(i),
			Populace: uint32(i),
		}
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
	const units = 100000
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
		zsl.GetRank(obj.Populace, obj)
	}
}

func BenchmarkZSkipListGetElementByRank(b *testing.B) {
	b.StopTimer()
	const units = 100000
	var set = makeTestData(units, 1000)
	var zsl = NewZSkipList(testRandSeed)
	for _, v := range set {
		var node = zsl.Insert(v.Populace, v)
		if node == nil {
			b.Fatalf("insert item[%d-%d] failed", v.Populace, v.uid)
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		var rank = (i % units) + 1
		zsl.GetElementByRank(rank)
	}
}
