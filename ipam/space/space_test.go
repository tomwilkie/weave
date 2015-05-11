package space

import (
	"testing"

	"github.com/weaveworks/weave/ipam/utils"
	wt "github.com/weaveworks/weave/testing"
)

func TestSpaceAllocate(t *testing.T) {
	const (
		testAddr1   = "10.0.3.4"
		testAddrx   = "10.0.3.19"
		testAddry   = "10.0.9.19"
		containerID = "deadbeef"
	)

	space1 := Space{Start: utils.ParseIP(testAddr1), Size: 20}
	wt.AssertEquals(t, space1.NumFreeAddresses(), utils.Offset(20))
	space1.assertInvariants()

	_, addr1 := space1.Allocate()
	wt.AssertEqualString(t, addr1.String(), testAddr1, "address")
	wt.AssertEquals(t, space1.NumFreeAddresses(), utils.Offset(19))
	space1.assertInvariants()

	_, addr2 := space1.Allocate()
	wt.AssertNotEqualString(t, addr2.String(), testAddr1, "address")
	wt.AssertEquals(t, space1.NumFreeAddresses(), utils.Offset(18))
	space1.assertInvariants()

	space1.Free(addr2)
	space1.assertInvariants()

	wt.AssertErrorInterface(t, space1.Free(addr2), (*error)(nil), "double free")
	wt.AssertErrorInterface(t, space1.Free(utils.ParseIP(testAddrx)), (*error)(nil), "address not allocated")
	wt.AssertErrorInterface(t, space1.Free(utils.ParseIP(testAddry)), (*error)(nil), "wrong out of range")

	space1.assertInvariants()
}

func TestSpaceFree(t *testing.T) {
	const (
		testAddr1   = "10.0.3.4"
		testAddrx   = "10.0.3.19"
		testAddry   = "10.0.9.19"
		containerID = "deadbeef"
	)

	space := Space{Start: utils.ParseIP(testAddr1), Size: 20}

	// Check we are prepared to give up the entire space
	start, size := space.BiggestFreeChunk()
	wt.AssertTrue(t, start == utils.ParseIP(testAddr1) && size == 20, "Wrong space")

	for i := 0; i < 20; i++ {
		ok, _ := space.Allocate()
		wt.AssertTrue(t, ok, "Failed to get address")
	}

	// Check we are full
	ok, _ := space.Allocate()
	wt.AssertTrue(t, !ok, "Should have failed to get address")
	start, size = space.BiggestFreeChunk()
	wt.AssertTrue(t, size == 0, "Wrong space")

	// Free in the middle
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.13")))
	start, size = space.BiggestFreeChunk()
	wt.AssertTrue(t, start == utils.ParseIP("10.0.3.13") && size == 1, "Wrong space")

	// Free one at the end
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.23")))
	start, size = space.BiggestFreeChunk()
	wt.AssertTrue(t, start == utils.ParseIP("10.0.3.23") && size == 1, "Wrong space")

	// Now free a few at the end
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.22")))
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.21")))

	wt.AssertEquals(t, space.NumFreeAddresses(), utils.Offset(4))

	// Now get the biggest free space; should be 3.21
	start, size = space.BiggestFreeChunk()
	wt.AssertTrue(t, start == utils.ParseIP("10.0.3.21") && size == 3, "Wrong space")

	// Now free a few in the middle
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.12")))
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.11")))
	wt.AssertSuccess(t, space.Free(utils.ParseIP("10.0.3.10")))

	wt.AssertEquals(t, space.NumFreeAddresses(), utils.Offset(7))

	// Now get the biggest free space; should be 3.21
	start, size = space.BiggestFreeChunk()
	wt.AssertTrue(t, start == utils.ParseIP("10.0.3.10") && size == 4, "Wrong space")
}

func TestSpaceSplit(t *testing.T) {
	const (
		containerID = "feedbacc"
		testAddr1   = "10.0.1.1"
		testAddr2   = "10.0.1.3"
	)

	space1 := Space{Start: utils.ParseIP(testAddr1), Size: 10}
	_, addr1 := space1.Allocate()
	_, addr2 := space1.Allocate()
	_, addr3 := space1.Allocate()
	space1.Free(addr2)
	space1.assertInvariants()
	split1, split2 := space1.Split(utils.ParseIP(testAddr2))
	wt.AssertEquals(t, split1.Size, utils.Offset(2))
	wt.AssertEquals(t, split2.Size, utils.Offset(8))
	wt.AssertEquals(t, split1.NumFreeAddresses(), utils.Offset(1))
	wt.AssertEquals(t, split2.NumFreeAddresses(), utils.Offset(7))
	space1.assertInvariants()
	split1.assertInvariants()
	split2.assertInvariants()
	wt.AssertNoErr(t, split1.Free(addr1))
	wt.AssertErrorInterface(t, split1.Free(addr3), (*error)(nil), "free")
	wt.AssertErrorInterface(t, split2.Free(addr1), (*error)(nil), "free")
	wt.AssertNoErr(t, split2.Free(addr3))
}
