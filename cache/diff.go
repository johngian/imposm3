package cache

import (
	"context"
	"runtime"
	"sort"
	"strconv"
	"sync"

	"github.com/go-redis/redis/v8"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/cache/binary"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/log"
)

type DiffCache struct {
	Coords    *CoordsRefIndex    // Stores which ways a coord references
	CoordsRel *CoordsRelRefIndex // Stores which relations a coord references
	Ways      *WaysRefIndex      // Stores which relations a way references
	opened    bool
}

func NewDiffCache() *DiffCache {
	cache := &DiffCache{}
	return cache
}

func (c *DiffCache) Close() {
	if c.Coords != nil {
		c.Coords.Close()
		c.Coords = nil
	}
	if c.CoordsRel != nil {
		c.CoordsRel.Close()
		c.CoordsRel = nil
	}
	if c.Ways != nil {
		c.Ways.Close()
		c.Ways = nil
	}
}

func (c *DiffCache) Flush() {
	if c.Coords != nil {
		c.Coords.Flush()
	}
	if c.CoordsRel != nil {
		c.CoordsRel.Flush()
	}
	if c.Ways != nil {
		c.Ways.Flush()
	}
}

func (c *DiffCache) Open() error {
	var err error
	c.Coords, err = newCoordsRefIndex()
	if err != nil {
		c.Close()
		return err
	}
	c.CoordsRel, err = newCoordsRelRefIndex()
	if err != nil {
		c.Close()
		return err
	}
	c.Ways, err = newWaysRefIndex()
	if err != nil {
		c.Close()
		return err
	}
	c.opened = true
	return nil
}

func (c *DiffCache) Exists() bool {
	return c.opened
}

func (c *DiffCache) Remove() error {
	err := c.Coords.db.FlushAll(context.TODO()).Err()

	if err != nil {
		return err
	}
	if c.opened {
		c.Close()
	}
	return nil
}

const bufferSize = 64 * 1024

type idRef struct {
	id  int64
	ref int64
}

// idRefBunch stores multiple IDRefs
type idRefBunch struct {
	id     int64 // the bunch id
	idRefs []element.IDRefs
}

// idRefBunches can hold multiple idRefBunch
type idRefBunches map[int64]idRefBunch

func (bunches *idRefBunches) add(bunchID, id, ref int64) {
	idRefs := bunches.getCreate(bunchID, id)
	idRefs.Add(ref)
}

func (bunches *idRefBunches) getCreate(bunchID, id int64) *element.IDRefs {
	bunch, ok := (*bunches)[bunchID]
	if !ok {
		bunch = idRefBunch{id: bunchID}
	}
	result := bunch.getCreate(id)

	(*bunches)[bunchID] = bunch
	return result
}

func (bunch *idRefBunch) get(id int64) *element.IDRefs {
	var result *element.IDRefs

	i := sort.Search(len(bunch.idRefs), func(i int) bool {
		return bunch.idRefs[i].ID >= id
	})
	if i < len(bunch.idRefs) && bunch.idRefs[i].ID == id {
		result = &bunch.idRefs[i]
	}
	return result
}

func (bunch *idRefBunch) getCreate(id int64) *element.IDRefs {
	var result *element.IDRefs

	i := sort.Search(len(bunch.idRefs), func(i int) bool {
		return bunch.idRefs[i].ID >= id
	})
	if i < len(bunch.idRefs) && bunch.idRefs[i].ID >= id {
		if bunch.idRefs[i].ID == id {
			result = &bunch.idRefs[i]
		} else {
			bunch.idRefs = append(bunch.idRefs, element.IDRefs{})
			copy(bunch.idRefs[i+1:], bunch.idRefs[i:])
			bunch.idRefs[i] = element.IDRefs{ID: id}
			result = &bunch.idRefs[i]
		}
	} else {
		bunch.idRefs = append(bunch.idRefs, element.IDRefs{ID: id})
		result = &bunch.idRefs[len(bunch.idRefs)-1]
	}

	return result
}

var idRefBunchesPool chan idRefBunches

func init() {
	idRefBunchesPool = make(chan idRefBunches, 1)
}

// bunchRefCache
type bunchRefCache struct {
	cache
	linearImport bool
	buffer       idRefBunches
	write        chan idRefBunches
	addc         chan idRef
	mu           sync.Mutex
	waitAdd      sync.WaitGroup
	waitWrite    sync.WaitGroup
}

func newRefIndex(db int) (*bunchRefCache, error) {
	index := bunchRefCache{}
	err := index.open(db)
	if err != nil {
		return nil, err
	}
	index.write = make(chan idRefBunches, 2)
	index.buffer = make(idRefBunches, bufferSize)
	index.addc = make(chan idRef, 1024)

	return &index, nil
}

type CoordsRefIndex struct {
	*bunchRefCache
}
type CoordsRelRefIndex struct {
	*bunchRefCache
}
type WaysRefIndex struct {
	*bunchRefCache
}

func newCoordsRefIndex() (*CoordsRefIndex, error) {
	cache, err := newRefIndex(14)
	if err != nil {
		return nil, err
	}
	return &CoordsRefIndex{cache}, nil
}

func newCoordsRelRefIndex() (*CoordsRelRefIndex, error) {
	cache, err := newRefIndex(15)
	if err != nil {
		return nil, err
	}
	return &CoordsRelRefIndex{cache}, nil
}

func newWaysRefIndex() (*WaysRefIndex, error) {
	cache, err := newRefIndex(16)
	if err != nil {
		return nil, err
	}
	return &WaysRefIndex{cache}, nil
}

func (index *bunchRefCache) getBunchID(id int64) int64 {
	return id / 64
}

func (index *bunchRefCache) Flush() {
	if index.linearImport {
		// disable linear import flushes buffer
		index.SetLinearImport(false)
		index.SetLinearImport(true)
	}
}

func (index *bunchRefCache) Close() {
	if index.linearImport {
		// disable linear import first to flush buffer
		index.SetLinearImport(false)
	}

	index.cache.Close()
}

func (index *bunchRefCache) Get(id int64) []int64 {
	if index.linearImport {
		panic("programming error: get not supported in linearImport mode")
	}

	bunch_id := index.getBunchID(id)
	data, err := index.db.Get(context.TODO(), strconv.FormatInt(bunch_id, 10)).Result()
	if err != nil && err != redis.Nil {
		panic(err)
	}

	if err != redis.Nil {
		idRefs := idRefsPool.get()
		defer idRefsPool.release(idRefs)
		for _, idRef := range binary.UnmarshalIDRefsBunch2([]byte(data), idRefs) {
			if idRef.ID == id {
				return idRef.Refs
			}
		}
	}
	return nil
}

func (index *bunchRefCache) Add(id, ref int64) error {
	bunch_id := index.getBunchID(id)
	data, err := index.db.Get(context.TODO(), strconv.FormatInt(bunch_id, 10)).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	var idRefs []element.IDRefs
	if err != redis.Nil {
		idRefs = idRefsPool.get()
		defer idRefsPool.release(idRefs)
		idRefs = binary.UnmarshalIDRefsBunch2([]byte(data), idRefs)
	}

	idRefBunch := idRefBunch{index.getBunchID(id), idRefs}
	idRef := idRefBunch.getCreate(id)
	idRef.Add(ref)

	byte_data := bytePool.get()
	defer bytePool.release(byte_data)
	byte_data = binary.MarshalIDRefsBunch2(idRefBunch.idRefs, byte_data)

	return index.db.Set(context.TODO(), strconv.FormatInt(bunch_id, 10), byte_data, 0).Err()
}

func (index *bunchRefCache) DeleteRef(id, ref int64) error {
	if index.linearImport {
		panic("programming error: delete not supported in linearImport mode")
	}

	bunch_id := index.getBunchID(id)

	data, err := index.db.Get(context.TODO(), strconv.FormatInt(bunch_id, 10)).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if err != redis.Nil {
		idRefs := idRefsPool.get()
		defer idRefsPool.release(idRefs)
		idRefs = binary.UnmarshalIDRefsBunch2([]byte(data), idRefs)
		idRefBunch := idRefBunch{index.getBunchID(id), idRefs}
		idRef := idRefBunch.get(id)
		if idRef != nil {
			idRef.Delete(ref)
			data := bytePool.get()
			defer bytePool.release(data)
			data = binary.MarshalIDRefsBunch2(idRefs, data)
			return index.db.Set(context.TODO(), strconv.FormatInt(bunch_id, 10), data, 0).Err()
		}
	}
	return nil
}

func (index *bunchRefCache) Delete(id int64) error {
	if index.linearImport {
		panic("programming error: delete not supported in linearImport mode")
	}

	bunch_id := index.getBunchID(id)

	data, err := index.db.Get(context.TODO(), strconv.FormatInt(bunch_id, 10)).Result()
	if err != nil && err != redis.Nil {
		return err
	}

	if err != redis.Nil {
		idRefs := idRefsPool.get()
		defer idRefsPool.release(idRefs)
		idRefs = binary.UnmarshalIDRefsBunch2([]byte(data), idRefs)
		idRefBunch := idRefBunch{index.getBunchID(id), idRefs}
		idRef := idRefBunch.get(id)
		if idRef != nil {
			idRef.Refs = []int64{}
			data := bytePool.get()
			defer bytePool.release(data)
			data = binary.MarshalIDRefsBunch2(idRefs, data)
			return index.db.Set(context.TODO(), strconv.FormatInt(bunch_id, 10), data, 0).Err()
		}
	}
	return nil
}

func (index *CoordsRefIndex) AddFromWay(way *osm.Way) {
	for _, node := range way.Nodes {
		if index.linearImport {
			index.addc <- idRef{id: node.ID, ref: way.ID}
		} else {
			index.Add(node.ID, way.ID)
		}
	}
}

func (index *CoordsRefIndex) DeleteFromWay(way *osm.Way) {
	if index.linearImport {
		panic("programming error: delete not supported in linearImport mode")
	}
	for _, node := range way.Nodes {
		index.DeleteRef(node.ID, way.ID)
	}
}

func (index *CoordsRelRefIndex) AddFromMembers(relID int64, members []osm.Member) {
	for _, member := range members {
		if member.Type == osm.NodeMember {
			if index.linearImport {
				index.addc <- idRef{id: member.ID, ref: relID}
			} else {
				index.Add(member.ID, relID)
			}
		}
	}
}

func (index *WaysRefIndex) AddFromMembers(relID int64, members []osm.Member) {
	for _, member := range members {
		if member.Type == osm.WayMember {
			if index.linearImport {
				index.addc <- idRef{id: member.ID, ref: relID}
			} else {
				index.Add(member.ID, relID)
			}
		}
	}
}

// SetLinearImport optimizes the cache for write operations.
// Get/Delete operations will panic during linear import.
func (index *bunchRefCache) SetLinearImport(val bool) {
	if val == index.linearImport {
		// already in this mode
		return
	}
	if val {
		index.waitWrite.Add(1)
		index.waitAdd.Add(1)

		go index.writer()
		go index.dispatch()

		index.linearImport = true
	} else {
		close(index.addc)
		index.waitAdd.Wait()
		close(index.write)
		index.waitWrite.Wait()

		index.linearImport = false
	}
}

func (index *bunchRefCache) writer() {
	for buffer := range index.write {
		if err := index.writeRefs(buffer); err != nil {
			log.Println("[error] writing ref index:", err)
		}
	}
	index.waitWrite.Done()
}

func (index *bunchRefCache) dispatch() {
	for idRef := range index.addc {
		index.buffer.add(index.getBunchID(idRef.id), idRef.id, idRef.ref)
		if len(index.buffer) >= bufferSize {
			index.write <- index.buffer
			select {
			case index.buffer = <-idRefBunchesPool:
			default:
				index.buffer = make(idRefBunches, bufferSize)
			}
		}
	}
	if len(index.buffer) > 0 {
		index.write <- index.buffer
		index.buffer = nil
	}
	index.waitAdd.Done()
}

type loadBunchItem struct {
	bunchID int64
	bunch   idRefBunch
}

type writeBunchItem struct {
	bunchID int64
	data    []byte
}

func (index *bunchRefCache) writeRefs(idRefs idRefBunches) error {
	pipeline := index.cache.db.Pipeline()
	wg := sync.WaitGroup{}
	putc := make(chan writeBunchItem)
	loadc := make(chan loadBunchItem)

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for item := range loadc {
				putc <- writeBunchItem{
					item.bunchID,
					index.loadMergeMarshal(item.bunchID, item.bunch.idRefs),
				}
			}
			wg.Done()
		}()
	}

	go func() {
		for bunchID, bunch := range idRefs {
			loadc <- loadBunchItem{bunchID, bunch}
		}
		close(loadc)
		wg.Wait()
		close(putc)
	}()

	for item := range putc {
		pipeline.Set(context.TODO(), strconv.FormatInt(item.bunchID, 10), item.data, 0)
		bytePool.release(item.data)
	}

	go func() {
		for k := range idRefs {
			delete(idRefs, k)
		}
		select {
		case idRefBunchesPool <- idRefs:
		}
	}()
	_, err := pipeline.Exec(context.TODO())
	return err
}

func mergeBunch(bunch, newBunch []element.IDRefs) []element.IDRefs {
	lastIdx := 0

NextIDRef:
	// for each new idRef...
	for _, newIDRefs := range newBunch {
		// search place in bunch
		for i := lastIdx; i < len(bunch); i++ {
			if bunch[i].ID == newIDRefs.ID {
				// id already present
				if len(newIDRefs.Refs) == 0 {
					// no new refs -> delete
					bunch = append(bunch[:i], bunch[i+1:]...)
				} else { // otherwise add refs
					for _, r := range newIDRefs.Refs {
						bunch[i].Add(r)
					}
				}
				lastIdx = i
				continue NextIDRef
			}
			if bunch[i].ID > newIDRefs.ID {
				// insert before
				if len(newIDRefs.Refs) > 0 {
					bunch = append(bunch, element.IDRefs{})
					copy(bunch[i+1:], bunch[i:])
					bunch[i] = newIDRefs
				}
				lastIdx = i
				continue NextIDRef
			}
		}
		// insert at the end
		if len(newIDRefs.Refs) > 0 {
			bunch = append(bunch, newIDRefs)
			lastIdx = len(bunch) - 1
		}
	}
	return bunch
}

// loadMergeMarshal loads an existing bunch, merges the IDRefs and
// marshals the result again.
func (index *bunchRefCache) loadMergeMarshal(bunch_id int64, newBunch []element.IDRefs) []byte {
	data, err := index.db.Get(context.TODO(), strconv.FormatInt(bunch_id, 10)).Result()
	if err != nil && err != redis.Nil {
		panic(err)
	}

	var bunch []element.IDRefs

	if err != redis.Nil {
		bunch = idRefsPool.get()
		defer idRefsPool.release(bunch)
		bunch = binary.UnmarshalIDRefsBunch2([]byte(data), bunch)
	}

	if bunch == nil {
		bunch = newBunch
	} else {
		bunch = mergeBunch(bunch, newBunch)
	}

	byte_data := bytePool.get()
	byte_data = binary.MarshalIDRefsBunch2(bunch, byte_data)
	return byte_data
}

// pools to reuse memory
var idRefsPool = make(idRefsPoolWrapper, 8)
var bytePool = make(bytePoolWrapper, 8)

type bytePoolWrapper chan []byte

func (p *bytePoolWrapper) get() []byte {
	select {
	case buf := <-(*p):
		return buf
	default:
		return nil
	}
}

func (p *bytePoolWrapper) release(buf []byte) {
	select {
	case (*p) <- buf:
	default:
	}
}

type idRefsPoolWrapper chan []element.IDRefs

func (p *idRefsPoolWrapper) get() []element.IDRefs {
	select {
	case idRefs := <-(*p):
		return idRefs
	default:
		return nil
	}
}

func (p *idRefsPoolWrapper) release(idRefs []element.IDRefs) {
	select {
	case (*p) <- idRefs:
	default:
	}
}
