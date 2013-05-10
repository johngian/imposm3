package stats

import (
	"fmt"
	"time"
)

type RpsCounter struct {
	counter int64
	lastAdd int64
	start   time.Time
	stop    time.Time
	updated bool
}

func (r *RpsCounter) Add(n int) {
	r.counter += int64(n)
	r.lastAdd += int64(n)
	if n > 0 {
		if r.start.IsZero() {
			r.start = time.Now()
		}
		r.updated = true
	}
}

func (r *RpsCounter) Value() int64 {
	return r.counter
}

func (r *RpsCounter) Rps(round int) int32 {
	rps := float64(r.counter) / float64(r.stop.Sub(r.start).Seconds())
	return int32(rps/float64(round)) * int32(round)
}

func (r *RpsCounter) LastRps(round int32) int32 {
	rps := float64(r.lastAdd) / float64(time.Since(r.stop).Seconds())
	return int32(rps/float64(round)) * int32(round)
}

func (r *RpsCounter) Tick() {
	if r.updated {
		r.stop = time.Now()
		r.updated = false
	}
	r.lastAdd = 0
}

type counter struct {
	start     time.Time
	coords    RpsCounter
	nodes     RpsCounter
	ways      RpsCounter
	relations RpsCounter
}

func (c *counter) Tick() {
	c.coords.Tick()
	c.nodes.Tick()
	c.ways.Tick()
	c.relations.Tick()
}

// Duration returns the duration since start with seconds precission.
func (c *counter) Duration() time.Duration {
	return time.Duration(int64(time.Since(c.start).Seconds()) * 1000 * 1000 * 1000)

}

type Statistics struct {
	coords    chan int
	nodes     chan int
	ways      chan int
	relations chan int
	reset     chan bool
	messages  chan string
}

func (s *Statistics) AddCoords(n int)    { s.coords <- n }
func (s *Statistics) AddNodes(n int)     { s.nodes <- n }
func (s *Statistics) AddWays(n int)      { s.ways <- n }
func (s *Statistics) AddRelations(n int) { s.relations <- n }
func (s *Statistics) Reset()             { s.reset <- true }
func (s *Statistics) Stop()              { s.reset <- false }
func (s *Statistics) Message(msg string) { s.messages <- msg }

func StatsReporter() *Statistics {
	c := counter{}
	c.start = time.Now()
	s := Statistics{}
	s.coords = make(chan int)
	s.nodes = make(chan int)
	s.ways = make(chan int)
	s.relations = make(chan int)
	s.reset = make(chan bool)
	s.messages = make(chan string)

	go func() {
		tick := time.Tick(time.Second)
		tock := time.Tick(time.Minute)
		for {
			select {
			case n := <-s.coords:
				c.coords.Add(n)
			case n := <-s.nodes:
				c.nodes.Add(n)
			case n := <-s.ways:
				c.ways.Add(n)
			case n := <-s.relations:
				c.relations.Add(n)
			case v := <-s.reset:
				if v {
					c.PrintStats()
					c = counter{}
					c.start = time.Now()
				} else {
					// stop
					c.PrintStats()
					return
				}
			case msg := <-s.messages:
				c.PrintTick()
				fmt.Println("\n", msg)
			case <-tock:
				c.PrintStats()
			case <-tick:
				c.PrintTick()
				c.Tick()
			}
		}
	}()
	return &s
}

func (c *counter) PrintTick() {
	fmt.Printf("\x1b[2K\r[%s] [%6s] C: %7d/s %7d/s (%10d) N: %7d/s %7d/s (%9d) W: %7d/s %7d/s (%8d) R: %6d/s %6d/s (%7d)",
		c.start.Format(time.Stamp),
		c.Duration(),
		c.coords.Rps(1000),
		c.coords.LastRps(1000),
		c.coords.Value(),
		c.nodes.Rps(100),
		c.nodes.LastRps(100),
		c.nodes.Value(),
		c.ways.Rps(100),
		c.ways.LastRps(100),
		c.ways.Value(),
		c.relations.Rps(10),
		c.relations.LastRps(10),
		c.relations.Value(),
	)
}

func (c *counter) PrintStats() {
	fmt.Printf("\x1b[2K\r[%s] [%6s] C: %7d/s (%10d) N: %7d/s (%9d) W: %7d/s (%8d) R: %6d/s (%7d)\n",
		c.start.Format(time.Stamp),
		c.Duration(),
		c.coords.Rps(1000),
		c.coords.Value(),
		c.nodes.Rps(100),
		c.nodes.Value(),
		c.ways.Rps(100),
		c.ways.Value(),
		c.relations.Rps(10),
		c.relations.Value(),
	)
}
