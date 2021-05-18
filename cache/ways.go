package cache

import (
	"context"
	"strconv"

	"github.com/go-redis/redis/v8"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
)

type WaysCache struct {
	cache
}

func newWaysCache(db int) (*WaysCache, error) {
	cache := WaysCache{}
	err := cache.open(db)
	if err != nil {
		return nil, err
	}
	return &cache, err
}

func (c *WaysCache) PutWay(way *osm.Way) error {
	if way.ID == SKIP {
		return nil
	}
	data, err := binary.MarshalWay(way)
	if err != nil {
		return err
	}
	return c.db.Set(context.TODO(), strconv.FormatInt(way.ID, 10), data, 0).Err()
}

func (c *WaysCache) PutWays(ways []osm.Way) error {
	pipeline := c.cache.db.Pipeline()
	for _, way := range ways {
		if way.ID == SKIP {
			continue
		}
		data, err := binary.MarshalWay(&way)
		if err != nil {
			return err
		}
		pipeline.Set(context.TODO(), strconv.FormatInt(way.ID, 10), data, 0)
	}
	_, err := pipeline.Exec(context.TODO())
	return err
}

func (c *WaysCache) GetWay(id int64) (*osm.Way, error) {
	data, err := c.db.Get(context.TODO(), strconv.FormatInt(id, 10)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	if err == redis.Nil {
		return nil, NotFound
	}
	way, err := binary.UnmarshalWay([]byte(data))
	if err != nil {
		return nil, err
	}
	way.ID = id
	return way, nil
}

func (c *WaysCache) DeleteWay(id int64) error {
	return c.cache.db.Del(context.TODO(), strconv.FormatInt(id, 10)).Err()
}

func (c *WaysCache) Iter() chan *osm.Way {
	ways := make(chan *osm.Way, 1024)
	go func() {
		var cursor uint64
		var n int
		for {
			var keys []string
			var err error
			keys, cursor, err = c.cache.db.Scan(context.TODO(), cursor, "*", 10).Result()
			if err != nil {
				panic(err)
			}
			n += len(keys)
			if cursor == 0 {
				close(ways)
				break
			}

			values, err := c.cache.db.MGet(context.TODO(), keys...).Result()
			if err != nil {
				panic(err)
			}

			for i, key := range keys {
				way, err := binary.UnmarshalWay([]byte(values[i].(string)))
				if err != nil {
					panic(err)
				}
				id, err := strconv.ParseInt(key, 10, 64)
				if err != nil {
					panic(err)
				}
				way.ID = id
				ways <- way
			}
		}

	}()
	return ways
}

func (c *WaysCache) FillMembers(members []osm.Member) error {
	if members == nil || len(members) == 0 {
		return nil
	}
	for i, member := range members {
		if member.Type != osm.WayMember {
			continue
		}
		way, err := c.GetWay(member.ID)
		if err != nil {
			return err
		}
		members[i].Way = way
	}
	return nil
}
