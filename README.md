# go-zskiplist
a golang implementation of redis zskiplist

一个redis里[zskiplist](https://github.com/antirez/redis/blob/3.2/src/t_zset.c)的golang实现，主要用于实现类似游戏里的排行榜功能。


## Usage

zskiplist里的数据按照升序排列，即head为最小的元素。

一个可以排行的对象需要满足下面的接口:

``` go
type RankInterface interface {

	//每个排行对象的唯一标识
	Uuid() uint64
}
```


## Example

```go
package main

import (
	"fmt"
	"time"

	zskiplist "github.com/ichenq/go-zskiplist"
)


//简单的角色信息示例
type rankPlayer struct {
	id    uint64
	name  string
	level uint32
	score uint32
}

func (p *rankPlayer) Uuid() uint64 {
	return p.id
}

func ExampleZSkipList() {
	var playerMap = make(map[uint64]*rankPlayer)
	var zsl = NewZSkipList()

	//简单的测试角色数据
	var p1 = &rankPlayer{id: 1001, name: "jack", level: 12, score: 2012}
	var p2 = &rankPlayer{id: 1002, name: "tom", level: 13, score: 2015}
	var p3 = &rankPlayer{id: 1003, name: "mike", level: 14, score: 2014}
	var p4 = &rankPlayer{id: 1004, name: "john", level: 11, score: 2014}
	var p5 = &rankPlayer{id: 1005, name: "kevin", level: 14, score: 2011}
	playerMap[p1.id] = p1
	playerMap[p2.id] = p2
	playerMap[p3.id] = p3
	playerMap[p4.id] = p4
	playerMap[p5.id] = p5

	//插入角色数据到zskiplist
	for _, v := range playerMap {
		zsl.Insert(v.score, v)
	}

	//打印调试信息
	fmt.Printf("%v\n", zsl)

	//获取角色的排行信息
	var rank = zsl.GetRank(p1.score, p1) // in ascend order
	var myRank = zsl.Len() - rank + 1    // get descend rank
	fmt.Printf("rank of %s: %d\n", p1.name, myRank)

	//根据排行获取角色信息
	var node = zsl.GetElementByRank(rank)
	var player = playerMap[node.Obj.Uuid()]
	fmt.Printf("rank at %d is: %s\n", rank, player.name)

	//遍历整个zskiplist
	zsl.Walk(true, func(rank int, v RankInterface) bool {
		fmt.Printf("rank %d: %v", v)
		return true
	})

	//从zskiplist中删除p1
	if zsl.Delete(p1.score, p1) == nil {
		// error handling
	}

	p1.score += 10
	if zsl.Insert(p1.score, p1) == nil {
		// error handling
	}
}

```
