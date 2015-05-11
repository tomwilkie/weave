package ipam

import (
	"fmt"
	"github.com/weaveworks/weave/ipam/utils"
)

type allocateResult struct {
	ok   bool
	addr utils.Address
}

type allocate struct {
	resultChan       chan<- allocateResult
	hasBeenCancelled func() bool
	ident            string
}

// Try returns true if the request is completed, false if pending
func (g *allocate) Try(alloc *Allocator) bool {
	if g.hasBeenCancelled() {
		g.Cancel()
		return true
	}

	// If we have previously stored an address for this container, return it.
	if addr, found := alloc.owned[g.ident]; found {
		g.resultChan <- allocateResult{true, addr}
		return true
	}

	if ok, addr := alloc.spaceSet.Allocate(); ok {
		alloc.debugln("Allocated", addr, "for", g.ident)
		alloc.addOwned(g.ident, addr)
		g.resultChan <- allocateResult{true, addr}
		return true
	}

	// out of space
	if donor, err := alloc.ring.ChoosePeerToAskForSpace(); err == nil {
		alloc.debugln("Decided to ask peer", donor, "for space")
		alloc.sendRequest(donor, msgSpaceRequest)
	}

	return false
}

func (g *allocate) Cancel() {
	g.resultChan <- allocateResult{false, 0}
}

func (g *allocate) String() string {
	return fmt.Sprintf("Allocate for %s", g.ident)
}

func (g *allocate) ForContainer(ident string) bool {
	return g.ident == ident
}
