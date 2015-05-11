package space

import (
	"bytes"
	"fmt"
	"sort"

	lg "github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/utils"
)

// Set is a set of spaces...
type Set struct {
	spaces []*Space
}

// For compatibility with sort
func (s Set) Len() int           { return len(s.spaces) }
func (s Set) Less(i, j int) bool { return s.spaces[i].Start < s.spaces[j].Start }
func (s Set) Swap(i, j int)      { panic("Should never be swapping spaces!") }

// Spaces returns the list of spaces in this space set.
func (s *Set) Spaces() []*Space {
	return s.spaces
}

func (s *Set) String() string {
	var buf bytes.Buffer
	if len(s.spaces) > 0 {
		fmt.Fprintf(&buf, "Address ranges we own:")
		for _, space := range s.spaces {
			fmt.Fprintf(&buf, "\n  %s", space)
		}
	} else {
		fmt.Fprintf(&buf, "No address ranges owned")
	}
	return buf.String()
}

// -------------------------------------------------

func (s *Set) assertInvariants() {
	utils.Assert(sort.IsSorted(s))
	// TODO invariant around not overlapping
}

// AddSpace adds a new space to this set.
func (s *Set) AddSpace(newspace *Space) {
	s.assertInvariants()
	defer s.assertInvariants()

	i := s.find(newspace.Start)
	utils.Assert(i == len(s.spaces) || s.spaces[i].Start != newspace.Start)

	s.spaces = append(s.spaces, &Space{}) // make space
	copy(s.spaces[i+1:], s.spaces[i:])    // move up
	s.spaces[i] = newspace                // put in new element
}

// Clear removes all spaces from this space set.  Used during node shutdown.
func (s *Set) Clear() {
	s.spaces = s.spaces[:0]
}

// Return the position of the space at or above start
func (s *Set) find(start utils.Address) int {
	return sort.Search(len(s.spaces), func(j int) bool {
		return s.spaces[j].Start >= start
	})
}

// Get returns the space found at start.
func (s *Set) Get(start utils.Address) (*Space, bool) {
	i := s.find(start)
	if i < len(s.spaces) && s.spaces[i].Start == start {
		return s.spaces[i], true
	}
	return nil, false
}

// NumFreeAddresses returns the total free address across
// all Spaces in this set.
func (s *Set) NumFreeAddresses() utils.Offset {
	// TODO: Optimize; perhaps maintain the count in allocate and free
	var freeAddresses utils.Offset
	for _, space := range s.spaces {
		freeAddresses += space.NumFreeAddresses()
	}
	return freeAddresses
}

// GiveUpSpace returns some large reasonably-sized chunk of free space.
// Normally because one of our peers has asked for it.
func (s *Set) GiveUpSpace() (utils.Address, utils.Offset, bool) {
	s.assertInvariants()
	defer s.assertInvariants()

	totalFreeAddresses := s.NumFreeAddresses()
	// Don't give away more than half the space we own, unless it's the very last address
	var maxDonation = totalFreeAddresses / 2
	if maxDonation < 1 {
		maxDonation = 1
	}

	// First find the biggest free chunk amongst all our spaces
	var bestStart utils.Address
	var bestSize utils.Offset
	var spaceIndex int
	for j, space := range s.spaces {
		chunkStart, chunkSize := space.BiggestFreeChunk()
		if chunkSize < bestSize {
			continue
		}

		bestStart, bestSize = chunkStart, chunkSize
		spaceIndex = j
	}

	if bestSize == 0 {
		utils.Assert(totalFreeAddresses == 0)
		return 0, 0, false
	}

	if bestSize > maxDonation {
		// Try and align the start to the right most
		bestStart = utils.Add(bestStart, bestSize-maxDonation)
		bestSize = maxDonation
	}

	utils.Assert(bestSize > 0)

	bestSpace := s.spaces[spaceIndex]
	lg.Debug.Println("GiveUpSpace start =", bestStart, "size =", bestSize, "from", bestSpace)

	// Now split and remove the final space
	utils.Assert(bestSpace.contains(bestStart))

	split1, split2 := bestSpace.Split(bestStart)
	var split3 *Space
	if split2.Size != bestSize {
		endAddress := utils.Add(bestStart, bestSize)
		split2, split3 = split2.Split(endAddress)
	}

	utils.Assert(split2.NumFreeAddresses() == bestSize)

	// Take out the old space, then add up to two new spaces.
	// Ordering of s.spaces is important.
	s.spaces = append(s.spaces[:spaceIndex], s.spaces[spaceIndex+1:]...)

	if split1.Size > 0 {
		s.AddSpace(split1)
	}
	if split3 != nil {
		s.AddSpace(split3)
	}

	return bestStart, bestSize, true
}

// Allocate calls allocate on each Space this set owns, until
// it gets an address.
func (s *Set) Allocate() (bool, utils.Address) {
	// TODO: Optimize; perhaps cache last-used space
	for _, space := range s.spaces {
		if ok, ret := space.Allocate(); ok {
			return ok, ret
		}
	}
	return false, 0
}

// Free returns the provided address to the
// Space that owns it.
func (s *Set) Free(addr utils.Address) error {
	for _, space := range s.spaces {
		if space.contains(addr) {
			return space.Free(addr)
		}
	}
	lg.Debug.Println("Address", addr, "not in range", s)
	return fmt.Errorf("IP %s address not in range", addr.String())
}

// Claim an address that we think we should own
func (s *Set) Claim(addr utils.Address) error {
	for _, space := range s.spaces {
		if done, err := space.Claim(addr); err != nil {
			return err
		} else if done {
			return nil
		}
	}
	return fmt.Errorf("IP %s address not in range", addr.String())
}
