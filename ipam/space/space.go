package space

import (
	"fmt"
	"math"

	"github.com/weaveworks/weave/ipam/utils"
	"golang.org/x/tools/container/intsets"
)

// Space repsents a range of addresses owned by this peer,
// and contains the state for managing free addresses.
type Space struct {
	Start utils.Address
	Size  utils.Offset
	inuse intsets.Sparse
}

const MaxSize = math.MaxInt32 // intsets.Sparse uses 'int' for indexing, so assume it might be 32-bit

func (space *Space) assertInvariants() {
	utils.Assert(space.inuse.Max() < int(space.Size))
}

func (space *Space) contains(addr utils.Address) bool {
	return addr >= space.Start && utils.Offset(addr-space.Start) < space.Size
}

// Claim marks an address as allocated on behalf of some specific container
func (space *Space) Claim(addr utils.Address) (bool, error) {
	if !space.contains(addr) {
		return false, nil
	}
	space.inuse.Insert(int(addr - space.Start))
	return true, nil
}

// Allocate returns the lowest availible IP within this space.
func (space *Space) Allocate() (bool, utils.Address) {
	space.assertInvariants()
	defer space.assertInvariants()

	// Find the lowest available address - would be better if intsets.Sparse did this internally
	var offset utils.Offset = 0
	for ; offset < space.Size; offset++ {
		if !space.inuse.Has(int(offset)) {
			break
		}
	}
	if offset >= space.Size { // out of space
		return false, 0
	}

	space.inuse.Insert(int(offset))
	return true, utils.Add(space.Start, offset)
}

// Free takes an IP in this space and record it as avalible.
func (space *Space) Free(addr utils.Address) error {
	space.assertInvariants()
	defer space.assertInvariants()

	if !space.contains(addr) {
		return fmt.Errorf("Free out of range: %s", addr.String())
	}

	offset := utils.Subtract(addr, space.Start)
	if !space.inuse.Has(int(offset)) {
		return fmt.Errorf("Freeing address that is not in use: %s", addr.String())
	}
	space.inuse.Remove(int(offset))

	return nil
}

// assertFree asserts that the size consequtive IPs from start
// (inclusive) are not allocated
func (space *Space) assertFree(start utils.Address, size utils.Offset) {
	utils.Assert(space.contains(start))
	utils.Assert(space.contains(utils.Add(start, size-1)))

	startOffset := int(utils.Subtract(start, space.Start))
	if startOffset > space.inuse.Max() { // Anything beyond this is free
		return
	}

	for i := startOffset; i < startOffset+int(size); i++ {
		utils.Assert(!space.inuse.Has(i))
	}
}

// BiggestFreeChunk scans the in-use list and returns the
// start, length of the largest free range of address it
// can find.  Length zero means it couldn't find any free space.
func (space *Space) BiggestFreeChunk() (utils.Address, utils.Offset) {
	space.assertInvariants()
	defer space.assertInvariants()

	if space.inuse.IsEmpty() { // Check to avoid Max() returning MinInt below
		return space.Start, space.Size
	}

	// Keep a track of the current chunk start and size
	// First chunk we've found is the one of unallocated space at the end
	max := space.inuse.Max()
	chunkStart := utils.Offset(max) + 1
	chunkSize := space.Size - chunkStart

	// Now scan the free list to find other chunks
	for i := 0; i < max; i++ {
		potentialStart := i
		// Run forward past all the free ones
		for i < max && !space.inuse.Has(i) {
			i++
		}
		// Is the chunk we found bigger than the
		// one we already have?
		potentialSize := utils.Offset(i - potentialStart)
		if potentialSize > chunkSize {
			chunkStart = utils.Offset(potentialStart)
			chunkSize = potentialSize
		}
	}

	// Now return what we found
	if chunkSize == 0 {
		return 0, 0
	}

	addr := utils.Add(space.Start, chunkStart)
	space.assertFree(addr, chunkSize)
	return addr, chunkSize
}

// Grow increases the size of this space to size.
func (space *Space) Grow(size utils.Offset) {
	utils.Assert(space.Size < size)
	space.Size = size
}

// NumFreeAddresses returns the total number of free addresses in
// this space.
func (space *Space) NumFreeAddresses() utils.Offset {
	return space.Size - utils.Offset(space.inuse.Len())
}

func (space *Space) String() string {
	return fmt.Sprintf("%s+%d (%d)", space.Start.String(), space.Size, space.inuse.Len())
}

// Split divide this space into two new spaces at a given address, copying allocations and frees.
func (space *Space) Split(addr utils.Address) (*Space, *Space) {
	utils.Assert(space.contains(addr))
	breakpoint := utils.Subtract(addr, space.Start)
	ret1 := &Space{Start: space.Start, Size: breakpoint}
	ret2 := &Space{Start: addr, Size: space.Size - breakpoint}

	// Now copy the in-use list - this implementation is slow but simple
	max := space.inuse.Max()
	for i := 0; i <= max; i++ {
		if space.inuse.Has(i) {
			if i < int(breakpoint) {
				ret1.inuse.Insert(i)
			} else {
				ret2.inuse.Insert(i - int(breakpoint))
			}
		}
	}

	return ret1, ret2
}
