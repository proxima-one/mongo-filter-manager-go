package driver

import (
	"context"
	"time"
)

type QueueCache map[string]map[string]CanBeStored

type ObjectQueue struct {
	channel        chan CanBeStored
	cacheSize      int
	cacheTimeout   time.Duration
	supportedTypes []string
	cache          QueueCache
	objectsInCache int
	workerContext  context.Context
	callback       func(cache QueueCache)
}

func NewQueue(supportedTypes []string, cacheSize int, cacheTimeout time.Duration) *ObjectQueue {
	res := ObjectQueue{}
	res.channel = make(chan CanBeStored, cacheSize)
	res.cacheSize = cacheSize
	res.supportedTypes = supportedTypes
	res.cacheTimeout = cacheTimeout
	res.initCache()
	return &res
}

func (q *ObjectQueue) StartProcessing(ctx context.Context, callback func(cache QueueCache)) {
	if ctx == nil {
		ctx = context.Background()
	}
	q.workerContext = ctx
	q.callback = callback
	go q.process(ctx)
}

func (q *ObjectQueue) StopProcessing() {
	q.workerContext.Done()
}

func (q *ObjectQueue) initCache() {
	q.cache = make(QueueCache)
	for _, supportedType := range q.supportedTypes {
		q.cache[supportedType] = make(map[string]CanBeStored, 0)
	}
	q.objectsInCache = 0
}

func (q *ObjectQueue) Push(obj CanBeStored) {
	q.channel <- obj
}

func (q *ObjectQueue) Save() {
	if q.callback != nil {
		q.callback(q.cache)
	}
	q.initCache()
}

func (q *ObjectQueue) AdjustCacheSize(size int) {
	//min size is 1
	if size < 1 {
		size = 1
	}
	q.cacheSize = size
}

func (q *ObjectQueue) process(ctx context.Context) {
	tick := time.NewTicker(q.cacheTimeout)
	for {
		select {
		case obj := <-q.channel:
			if _, ok := q.cache[obj.CollectionName()]; ok {
				if _, ok := q.cache[obj.CollectionName()][obj.GetId()]; !ok {
					q.objectsInCache++
				}
				q.cache[obj.CollectionName()][obj.GetId()] = obj
			}
			if q.objectsInCache < q.cacheSize {
				break
			}
			tick.Stop()
			q.Save()
			tick = time.NewTicker(q.cacheTimeout)

		case <-tick.C:
			q.Save()

		case <-ctx.Done():
			tick.Stop()
			q.Save()
			return
		}
	}
}
