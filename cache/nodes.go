package cache

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
)

type NodesCache struct {
	cache
}

func newNodesCache(db int) (*NodesCache, error) {
	cache := NodesCache{}
	err := cache.open(db)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *NodesCache) PutNode(node *osm.Node) error {
	if node.ID == SKIP {
		return nil
	}
	if node.Tags == nil {
		return nil
	}
	data, err := binary.MarshalNode(node)
	if err != nil {
		return err
	}
	return p.db.Set(context.TODO(), strconv.FormatInt(node.ID, 10), data, 0).Err()
}

func (p *NodesCache) PutNodes(nodes []osm.Node) (int, error) {
	pipeline := p.cache.db.Pipeline()

	var n int
	for _, node := range nodes {
		if node.ID == SKIP {
			continue
		}
		if len(node.Tags) == 0 {
			continue
		}
		data, err := binary.MarshalNode(&node)
		if err != nil {
			return 0, err
		}
		pipeline.Set(context.TODO(), strconv.FormatInt(node.ID, 10), data, 0)
		n++
	}

	_, err := pipeline.Exec(context.TODO())

	return n, err
}

func (p *NodesCache) GetNode(id int64) (*osm.Node, error) {
	data, err := p.db.Get(context.TODO(), strconv.FormatInt(id, 10)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, NotFound
	}
	node, err := binary.UnmarshalNode([]byte(data))
	if err != nil {
		return nil, err
	}
	node.ID = id
	return node, nil
}

func (p *NodesCache) DeleteNode(id int64) error {
	return p.cache.db.Del(context.TODO(), strconv.FormatInt(id, 10)).Err()
}

func (p *NodesCache) Iter() chan *osm.Node {
	nodes := make(chan *osm.Node)
	go func() {

		var cursor uint64
		var n int

		for {
			var keys []string
			var err error
			keys, cursor, err = p.cache.db.Scan(context.TODO(), cursor, "*", 10).Result()
			if err != nil {
				panic(err)
			}
			n += len(keys)
			if cursor == 0 {
				close(nodes)
				break
			}

			values, err := p.cache.db.MGet(context.TODO(), keys...).Result()
			if err != nil {
				panic(err)
			}

			for i, key := range keys {
				node, err := binary.UnmarshalNode([]byte(values[i].(string)))
				if err != nil {
					panic(err)
				}
				id, err := strconv.ParseInt(key, 10, 64)
				if err != nil {
					panic(err)
				}
				node.ID = id
				nodes <- node
			}
		}
	}()
	return nodes
}
