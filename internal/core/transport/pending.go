package transport

import "sync"

// pendingShards 分片数。requestID 是自增的, 按取模分片天然均匀
const pendingShards = 32

// pendingMap 保存 requestID → Future 的映射。
//
// 原来用 sync.Map, profile 显示它每次 Store 都要分配一个 entry 节点
// (占单次调用分配对象数的 4%)。这里的访问模式是"写一次、读一次、随即删除",
// 正是 sync.Map 最不擅长的形态(它为读多写少优化)。
// 换成分片的内建 map: 分片把锁竞争摊开, map 的桶可以复用, 不再有每请求的节点分配
type pendingMap struct {
	shards [pendingShards]pendingShard
}

type pendingShard struct {
	mu sync.Mutex
	m  map[uint64]*Future
}

func newPendingMap() *pendingMap {
	p := &pendingMap{}
	for i := range p.shards {
		p.shards[i].m = make(map[uint64]*Future, 64)
	}
	return p
}

func (p *pendingMap) shard(seq uint64) *pendingShard {
	return &p.shards[seq%pendingShards]
}

func (p *pendingMap) Store(seq uint64, f *Future) {
	s := p.shard(seq)
	s.mu.Lock()
	s.m[seq] = f
	s.mu.Unlock()
}

func (p *pendingMap) LoadAndDelete(seq uint64) (*Future, bool) {
	s := p.shard(seq)
	s.mu.Lock()
	f, ok := s.m[seq]
	if ok {
		delete(s.m, seq)
	}
	s.mu.Unlock()
	return f, ok
}

func (p *pendingMap) Delete(seq uint64) {
	s := p.shard(seq)
	s.mu.Lock()
	delete(s.m, seq)
	s.mu.Unlock()
}

// DrainAll 取出并清空所有在途请求, 用于连接失败时一次性让它们全部失败
func (p *pendingMap) DrainAll() []*Future {
	var futures []*Future
	for i := range p.shards {
		s := &p.shards[i]
		s.mu.Lock()
		for seq, f := range s.m {
			futures = append(futures, f)
			delete(s.m, seq)
		}
		s.mu.Unlock()
	}
	return futures
}
