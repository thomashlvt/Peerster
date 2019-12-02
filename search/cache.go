package search

import (
	"fmt"
	"sync"
	"time"
)

type reqID struct {
	origin string
	keywords string
}

type SearchRequestCache struct {
	*sync.RWMutex
	data map[reqID] time.Time
}

func NewSearchRequestCache() *SearchRequestCache {
	return &SearchRequestCache{
		RWMutex: &sync.RWMutex{},
		data:    make(map[reqID] time.Time),
	}
}

func (s *SearchRequestCache) Run() {
	go func() {
		t := time.NewTicker(time.Millisecond * 500)
		<- t.C
		s.Lock()
		for req, timeStamp := range s.data {
			if time.Since(timeStamp) > time.Millisecond * 500 {
				delete(s.data, req)
			}
		}
		s.Unlock()
	}()
}

func (s *SearchRequestCache) Check(origin string, keywords []string) bool {
	s.Lock()
	defer s.Unlock()

	id := reqID {
		origin:   origin,
		keywords: fmt.Sprintf("%v", keywords),
	}
	timeStamp, exists := s.data[id]

	if exists && time.Since(timeStamp) < time.Millisecond * 500 {
		return true // is duplicate
	} else {
		s.data[id] = time.Now()
		return false // no duplicate
	}
}