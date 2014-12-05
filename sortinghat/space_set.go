package sortinghat

import (
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/zettio/weave/router"
	"net"
	"sync"
)

// Represents our own space allocations.  See also PeerSpace.
type SpaceSet struct {
	router.PeerName
	version uint64
	spaces  []*Space
	sync.RWMutex
}

func NewSpaceSet(pn router.PeerName) *SpaceSet {
	return &SpaceSet{PeerName: pn}
}

func (s *SpaceSet) AddSpace(space *Space) {
	s.Lock()
	defer s.Unlock()
	s.spaces = append(s.spaces, space)
}

func (s *SpaceSet) Encode(enc *gob.Encoder) error {
	s.RLock()
	defer s.RUnlock()
	if err := enc.Encode(s.PeerName); err != nil {
		return err
	}
	if err := enc.Encode(s.version); err != nil {
		return err
	}
	if err := enc.Encode(len(s.spaces)); err != nil {
		return err
	}
	for _, space := range s.spaces {
		if err := enc.Encode(space.GetMinSpace()); err != nil {
			return err
		}
	}
	return nil
}

func (s *SpaceSet) String() string {
	s.RLock()
	defer s.RUnlock()
	return fmt.Sprint("SpaceSet ", s.PeerName, " (v", s.version, ") (spaces: ", len(s.spaces), ") (1st: ", s.spaces[0], ")")
}

func (s *SpaceSet) NumFreeAddresses() uint32 {
	s.RLock()
	defer s.RUnlock()
	// TODO: Optimize; perhaps maintain the count in allocate and free
	var freeAddresses uint32 = 0
	for _, space := range s.spaces {
		freeAddresses += space.NumFreeAddresses()
	}
	return freeAddresses
}

func (s *SpaceSet) AllocateFor(ident string) net.IP {
	s.Lock()
	defer s.Unlock()
	// TODO: Optimize; perhaps cache last-used space
	for _, space := range s.spaces {
		if ret := space.AllocateFor(ident); ret != nil {
			return ret
		}
	}
	return nil
}

func (s *SpaceSet) Free(addr net.IP) error {
	s.Lock()
	defer s.Unlock()
	for _, space := range s.spaces {
		if space.Free(addr) {
			return nil
		}
	}
	return errors.New("Attempt to free IP address not in range")
}