package storageTools

import (
	"KVwithWAL/config"
	"container/list"
	"fmt"
)

type CacheItem struct {
	key   int
	value []byte
}

type BlockCache struct {
	capacity int
	items    map[int]*list.Element
	order    *list.List
}

func NewBlockCache() *BlockCache {
	return &BlockCache{
		capacity: config.AppConfig.CacheSize,
		items:    make(map[int]*list.Element),
		order:    list.New(),
	}
}

func (c *BlockCache) Get(blockIndex int) ([]byte, bool) {
	if elem, found := c.items[blockIndex]; found {
		c.order.MoveToFront(elem)
		return elem.Value.(*CacheItem).value, true
	}
	return nil, false
}
func (c *BlockCache) Set(blockIndex int, data []byte) {
	if elem, found := c.items[blockIndex]; found {
		c.order.MoveToFront(elem)
		elem.Value.(*CacheItem).value = data
		return
	}
	if c.order.Len() == c.capacity {
		evict := c.order.Back()
		delete(c.items, evict.Value.(*CacheItem).key)
		c.order.Remove(evict)
	}
	item := &CacheItem{key: blockIndex, value: data}
	elem := c.order.PushFront(item)
	c.items[blockIndex] = elem
}

// remove after testing
func (c *BlockCache) PrintCacheState() {
	fmt.Println("Current cache state (most recent first):")
	for e := c.order.Front(); e != nil; e = e.Next() {
		item := e.Value.(*CacheItem)
		fmt.Printf("BlockIndex: %d\n", item.key)
	}
	fmt.Println("------")
}
