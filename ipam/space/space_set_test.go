package space

import (
	"net"
	"testing"

	"github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/utils"
	wt "github.com/weaveworks/weave/testing"
)

func equal(ms1 *Space, ms2 *Space) bool {
	return ms1.Start.Equal(ms2.Start) &&
		ms1.Size == ms2.Size
}

// Note: does not check version
func (ps1 *Set) Equal(ps2 *Set) bool {
	if len(ps1.spaces) == len(ps2.spaces) {
		for i := 0; i < len(ps1.spaces); i++ {
			if !equal(ps1.spaces[i], ps2.spaces[i]) {
				return false
			}
		}
		return true
	}
	return false
}

func spaceSetWith(spaces ...*Space) *Set {
	ps := Set{}
	for _, space := range spaces {
		ps.AddSpace(space)
	}
	return &ps
}

func TestGiveUpSimple(t *testing.T) {
	const (
		testAddr1 = "10.0.1.0"
		testAddr2 = "10.0.1.32"
	)

	var (
		ipAddr1 = net.ParseIP(testAddr1)
	)

	ps1 := spaceSetWith(&Space{Start: ipAddr1, Size: 48})

	// Empty space set should split in two and give me the second half
	start, numGivenUp, ok := ps1.GiveUpSpace()
	wt.AssertBool(t, ok, true, "GiveUpSpace result")
	wt.AssertEqualString(t, start.String(), "10.0.1.24", "Invalid start")
	wt.AssertEquals(t, numGivenUp, utils.Offset(24))
	wt.AssertEquals(t, ps1.NumFreeAddresses(), utils.Offset(24))

	// Now check we can give the rest up.
	count := 0 // count to avoid infinite loop
	for ; count < 1000; count++ {
		_, size, ok := ps1.GiveUpSpace()
		if !ok {
			break
		}
		numGivenUp += size
	}
	wt.AssertEquals(t, ps1.NumFreeAddresses(), utils.Offset(0))
	wt.AssertEquals(t, numGivenUp, utils.Offset(48))
}

func TestGiveUpHard(t *testing.T) {
	common.InitDefaultLogging(true)
	var (
		start              = net.ParseIP("10.0.1.0")
		size  utils.Offset = 48
	)

	// Fill a fresh space set
	spaceset := spaceSetWith(&Space{Start: start, Size: size})
	for i := utils.Offset(0); i < size; i++ {
		ip := spaceset.Allocate()
		wt.AssertTrue(t, ip != nil, "Failed to get IP!")
	}

	wt.AssertEquals(t, spaceset.NumFreeAddresses(), utils.Offset(0))

	// Now free all but the last address
	// this will force us to split the free list
	for i := utils.Offset(0); i < size-1; i++ {
		wt.AssertSuccess(t, spaceset.Free(utils.Add(start, i)))
	}

	// Now split
	newRange, numGivenUp, ok := spaceset.GiveUpSpace()
	wt.AssertBool(t, ok, true, "GiveUpSpace result")
	wt.AssertTrue(t, newRange.Equal(net.ParseIP("10.0.1.24")), "Invalid start")
	wt.AssertEquals(t, numGivenUp, utils.Offset(23))
	wt.AssertEquals(t, spaceset.NumFreeAddresses(), utils.Offset(24))

	//Space set should now have 2 spaces
	expected := spaceSetWith(&Space{Start: start, Size: 24},
		&Space{Start: net.ParseIP("10.0.1.47"), Size: 1})
	wt.AssertTrue(t, spaceset.Equal(expected), "Wrong sets")
}
