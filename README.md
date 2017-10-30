# go-zskiplist
a golang implementation of redis zskiplist

一个redis里[zskiplist](https://github.com/antirez/redis/blob/3.2/src/t_zset.c)的golang实现，用于实现游戏项目里的排行榜功能。


## Usage

zskiplist里的数据使用升序排列。

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
type Player struct {
	id    uint64
	name  string
	level uint32
	score uint32
}

func (p *testPlayer) Uuid() uint64 {
	return p.id
}


func main() {
	var zsl = zskiplist.NewZSkipList(time.Now().Unix())
	var p1 = &Player{uid: 1001, name: "jack", level: 12, score: 2012}
	var p2 = &Player{uid: 1002, name: "tome", level: 13, score: 2015}
	var p3 = &Player{uid: 1003, name: "mike", level: 14, score: 2014}

	//插入数据zskiplist
	zsl.Insert(p1.score, p1)
	zsl.Insert(p2.score, p2)
	zsl.Insert(p3.score, p3)

	// 调试打印
	fmt.Printf("%v\n", zsl)

	//获取角色的排行信息
	var rank = zsl.GetRank(p1.score, p1)

	//根据排行获取角色信息
	var node = zsl.GetElementByRank(rank)

	//遍历整个zskiplist，lambda返回false迭代结束
	zsl.Walk(true, func(rank int, v zskiplist.RankInterface) bool {
		// v is at position rank
		return true
	})

	//删除角色信息
	if node := zsl.Delete(p1.score, p1); node == nil {
		// error handling
	}
	p1.score += 100

	//分数更改后再次插入zskiplist
	if node := zsl.Insert(p1.score, p1); node == nil {
		// error handling
	}
}

```
