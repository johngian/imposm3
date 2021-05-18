package cache

import (
	"context"
	bin "encoding/binary"
	"errors"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
	osm "github.com/omniscale/go-osm"
)

var (
	NotFound = errors.New("not found")
)

const SKIP int64 = -1

type OSMCache struct {
	Coords    *DeltaCoordsCache
	Ways      *WaysCache
	Nodes     *NodesCache
	Relations *RelationsCache
	opened    bool
}

func (c *OSMCache) Close() {
	if c.Coords != nil {
		c.Coords.Close()
		c.Coords = nil
	}
	if c.Nodes != nil {
		c.Nodes.Close()
		c.Nodes = nil
	}
	if c.Ways != nil {
		c.Ways.Close()
		c.Ways = nil
	}
	if c.Relations != nil {
		c.Relations.Close()
		c.Relations = nil
	}
}

func NewOSMCache() *OSMCache {
	cache := &OSMCache{}
	return cache
}

func (c *OSMCache) Open() error {

	var err error
	c.Coords, err = newDeltaCoordsCache(10)
	if err != nil {
		return err
	}
	c.Nodes, err = newNodesCache(11)
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = newWaysCache(12)
	if err != nil {
		c.Close()
		return err
	}
	c.Relations, err = newRelationsCache(13)
	if err != nil {
		c.Close()
		return err
	}
	c.opened = true
	return nil
}

func (c *OSMCache) Exists() bool {
	return c.opened
}

func (c *OSMCache) Remove() error {
	err := c.Coords.db.FlushAll(context.TODO()).Err()

	if err != nil {
		return err
	}

	if c.opened {
		c.Close()
	}

	return nil
}

// FirstMemberIsCached checks whether the first way or node member is cached.
// Also returns true if there are no members of type WayMember or NodeMember.
func (c *OSMCache) FirstMemberIsCached(members []osm.Member) (bool, error) {
	for _, m := range members {
		if m.Type == osm.WayMember {
			_, err := c.Ways.GetWay(m.ID)
			if err == NotFound {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		} else if m.Type == osm.NodeMember {
			_, err := c.Coords.GetCoord(m.ID)
			if err == NotFound {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return true, nil
		}
	}
	return true, nil
}

type cache struct {
	db *redis.Client
}

func (c *cache) open(db int) error {

	env_url, exists := os.LookupEnv("IMPOSM_CACHE_URL")

	if !exists {
		panic("IMPOSM_CACHE_URL is undefined")
	}

	cache_url := fmt.Sprintf("%s/%d", env_url, db)

	opt, err := redis.ParseURL(cache_url)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(opt)
	c.db = rdb
	return nil
}

func idToKeyBuf(id int64) []byte {
	b := make([]byte, 8)
	bin.BigEndian.PutUint64(b, uint64(id))
	return b[:8]
}

func idFromKeyBuf(buf []byte) int64 {
	return int64(bin.BigEndian.Uint64(buf))
}

func (c *cache) Close() {
	c.db.Close()
}
