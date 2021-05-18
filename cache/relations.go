package cache

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
)

type RelationsCache struct {
	cache
}

func newRelationsCache(db int) (*RelationsCache, error) {
	cache := RelationsCache{}
	err := cache.open(db)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (p *RelationsCache) PutRelation(relation *osm.Relation) error {
	if relation.ID == SKIP {
		return nil
	}
	data, err := binary.MarshalRelation(relation)
	if err != nil {
		return err
	}
	return p.db.Set(context.TODO(), strconv.FormatInt(relation.ID, 10), data, 0).Err()
}

func (p *RelationsCache) PutRelations(rels []osm.Relation) error {
	pipeline := p.cache.db.Pipeline()

	for _, rel := range rels {
		if rel.ID == SKIP {
			continue
		}
		if len(rel.Tags) == 0 {
			continue
		}
		data, err := binary.MarshalRelation(&rel)
		if err != nil {
			return err
		}
		pipeline.Set(context.TODO(), strconv.FormatInt(rel.ID, 10), data, 0)
	}
	_, err := pipeline.Exec(context.TODO())
	return err
}

func (p *RelationsCache) Iter() chan *osm.Relation {
	rels := make(chan *osm.Relation)
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
				close(rels)
				break
			}
			values, err := p.cache.db.MGet(context.TODO(), keys...).Result()
			if err != nil {
				panic(err)
			}
			for i, key := range keys {
				rel, err := binary.UnmarshalRelation([]byte(values[i].(string)))
				if err != nil {
					panic(err)
				}
				id, err := strconv.ParseInt(key, 10, 64)
				if err != nil {
					panic(err)
				}
				rel.ID = id
				rels <- rel
			}

		}
	}()
	return rels
}

func (p *RelationsCache) GetRelation(id int64) (*osm.Relation, error) {
	data, err := p.db.Get(context.TODO(), strconv.FormatInt(id, 10)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, NotFound
	}
	relation, err := binary.UnmarshalRelation([]byte(data))
	if err != nil {
		return nil, err
	}
	relation.ID = id
	return relation, err
}

func (p *RelationsCache) DeleteRelation(id int64) error {
	return p.cache.db.Del(context.TODO(), strconv.FormatInt(id, 10)).Err()
}
