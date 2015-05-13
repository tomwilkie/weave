/*
Package ring implements a simple ring CRDT.
*/
package ring

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sort"
	"time"

	"github.com/weaveworks/weave/common"
	"github.com/weaveworks/weave/ipam/utils"
	"github.com/weaveworks/weave/router"
)

const maxClockSkew int64 = int64(time.Hour)

// Hook for replacing for testing
var now = func() int64 { return time.Now().Unix() }

// Ring represents the ring itself
type Ring struct {
	Now        int64           // When we send this ring to someone we include the time to help detect clock skew
	Start, End utils.Address   // [min, max) tokens in this ring.  Due to wrapping, min == max (effectively)
	Peername   router.PeerName // name of peer owning this ring instance
	Entries    entries         // list of entries sorted by token
}

func (r *Ring) assertInvariants() {
	err := r.checkInvariants()
	if err != nil {
		panic(err.Error())
	}
}

// Errors returned by merge
var (
	ErrNotSorted        = errors.New("Ring not sorted")
	ErrTokenRepeated    = errors.New("Token appears twice in ring")
	ErrTokenOutOfRange  = errors.New("Token is out of range")
	ErrDifferentSubnets = errors.New("IP Allocator with different subnet detected")
	ErrNewerVersion     = errors.New("Received new version for entry I own!")
	ErrInvalidEntry     = errors.New("Received invalid state update!")
	ErrEntryInMyRange   = errors.New("Received new entry in my range!")
	ErrNoFreeSpace      = errors.New("No free space found!")
	ErrTooMuchFreeSpace = errors.New("Entry reporting too much free space!")
	ErrInvalidTimeout   = errors.New("dt must be greater than 0")
	ErrNotFound         = errors.New("No entries for peer found")
	ErrClockSkew        = errors.New("Large clock skew detected; refusing to merge.")
)

func (r *Ring) checkInvariants() error {
	if !sort.IsSorted(r.Entries) {
		return ErrNotSorted
	}

	// Check no token appears twice
	// We know it's sorted...
	for i := 1; i < len(r.Entries); i++ {
		if r.Entries[i-1].Token == r.Entries[i].Token {
			return ErrTokenRepeated
		}
	}

	if len(r.Entries) == 0 {
		return nil
	}

	// Check tokens are in range
	if r.Entries.entry(0).Token < r.Start {
		return ErrTokenOutOfRange
	}
	if r.Entries.entry(-1).Token >= r.End {
		return ErrTokenOutOfRange
	}

	// Check all the freespaces are in range
	for i, entry := range r.Entries {
		next := r.Entries.entry(i + 1)
		distance := r.distance(entry.Token, next.Token)

		if entry.Free > distance {
			return ErrTooMuchFreeSpace
		}
	}

	return nil
}

// New creates an empty ring belonging to peer.
func New(start, end utils.Address, peer router.PeerName) *Ring {
	utils.Assert(start < end)

	ring := &Ring{Start: start, End: end, Peername: peer, Entries: make([]*entry, 0)}
	ring.updateExportedVariables()
	return ring
}

// TotalRemoteFree returns the approximate number of free IPs
// on other hosts.
func (r *Ring) TotalRemoteFree() utils.Offset {
	result := utils.Offset(0)
	for _, entry := range r.Entries {
		if entry.Peer != r.Peername {
			result += entry.Free
		}
	}
	return result
}

// Returns the distance between two tokens on this ring, dealing
// with ranges which cross the origin
func (r *Ring) distance(start, end utils.Address) utils.Offset {
	if end > start {
		return utils.Offset(end - start)
	}

	return utils.Offset((r.End - start) + (end - r.Start))
}

// GrantRangeToHost modifies the ring such that range [start, end)
// is assigned to peer.  This may insert up to two new tokens.
// Preconditions:
// - start < end
// - [start, end) must be owned by the calling peer
func (r *Ring) GrantRangeToHost(start, end utils.Address, peer router.PeerName) {
	//fmt.Printf("%s GrantRangeToHost [%v,%v) -> %s\n", r.Peername, start, end, peer)

	r.assertInvariants()
	defer r.assertInvariants()
	defer r.updateExportedVariables()

	// ----------------- Start of Checks -----------------

	utils.Assert(start < end)
	utils.Assert(r.Start <= start && start < r.End)
	utils.Assert(r.Start < end && end <= r.End)
	utils.Assert(len(r.Entries) > 0)

	// Look for the left-most entry greater than start, then go one previous
	// to get the right-most entry less than or equal to start
	preceedingPos := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token > start
	})
	preceedingPos--

	// Check all tokens up to end are owned by us
	for pos := preceedingPos; pos < len(r.Entries) && r.Entries.entry(pos).Token < end; pos++ {
		utils.Assert(r.Entries.entry(pos).Peer == r.Peername)
	}

	// ----------------- End of Checks -----------------

	// Free space at start is max(length of range, distance to next token)
	startFree := r.distance(start, r.Entries.entry(preceedingPos+1).Token)
	if length := r.distance(start, end); startFree > length {
		startFree = length
	}
	// Is there already a token at start, update it
	if previousEntry := r.Entries.entry(preceedingPos); previousEntry.Token == start {
		previousEntry.update(peer, startFree)
	} else {
		// Otherwise, these isn't a token here, insert a new one.
		r.Entries.insert(entry{Token: start, Peer: peer, Free: startFree})
		preceedingPos++
		// Reset free space on previous entry, which we own.
		previousEntry.update(r.Peername, r.distance(previousEntry.Token, start))
	}

	// Give all intervening tokens to the other peer
	pos := preceedingPos + 1
	for ; pos < len(r.Entries) && r.Entries.entry(pos).Token < end; pos++ {
		entry := r.Entries.entry(pos)
		entry.update(peer, entry.Free)
	}

	// There is never an entry with a token of r.End, as the end of
	// the ring is exclusive.
	if end == r.End {
		end = r.Start
	}

	//  If there is a token equal to the end of the range, we don't need to do anything further
	if _, found := r.Entries.get(end); found {
		return
	}

	// If not, we need to insert a token such that we claim this bit on the end.
	endFree := r.distance(end, r.Entries.entry(pos).Token)
	r.Entries.insert(entry{Token: end, Peer: r.Peername, Free: endFree})
}

// Merge the given ring into this ring and return any new ranges added
func (r *Ring) merge(gossip Ring) error {
	r.assertInvariants()
	defer r.assertInvariants()
	defer r.updateExportedVariables()

	// Don't panic when checking the gossiped in ring.
	// In this case just return any error found.
	if err := gossip.checkInvariants(); err != nil {
		return err
	}

	if r.Start != gossip.Start || r.End != gossip.End {
		return ErrDifferentSubnets
	}

	// Now merge their ring with yours, in a temporary ring.
	var result entries
	addToResult := func(e entry) { result = append(result, &e) }

	var mine, theirs *entry
	var previousOwner *router.PeerName
	// i is index into r.Entries; j is index into gossip.Entries
	var i, j int
	for i < len(r.Entries) && j < len(gossip.Entries) {
		mine, theirs = r.Entries[i], gossip.Entries[j]
		switch {
		case mine.Token < theirs.Token:
			addToResult(*mine)
			previousOwner = &mine.Peer
			i++
		case mine.Token > theirs.Token:
			// insert, checking that a range owned by us hasn't been split
			if previousOwner != nil && *previousOwner == r.Peername && theirs.Peer != r.Peername {
				return ErrEntryInMyRange
			}
			addToResult(*theirs)
			previousOwner = nil
			j++
		case mine.Token == theirs.Token:
			// merge
			switch {
			case mine.Version >= theirs.Version:
				if mine.Version == theirs.Version && !mine.Equal(theirs) {
					common.Debug.Printf("Error merging entries at %s - %v != %v\n", utils.AddressIP4(mine.Token), mine, theirs)
					return ErrInvalidEntry
				}
				addToResult(*mine)
				previousOwner = &mine.Peer
			case mine.Version < theirs.Version:
				if mine.Peer == r.Peername { // We shouldn't receive updates to our own tokens
					return ErrNewerVersion
				}
				addToResult(*theirs)
				previousOwner = nil
			}
			i++
			j++
		}
	}

	// At this point, either i is at the end of r or j is at the end
	// of gossip, so copy over the remaining entries.

	for ; i < len(r.Entries); i++ {
		mine = r.Entries[i]
		addToResult(*mine)
	}

	for ; j < len(gossip.Entries); j++ {
		theirs = gossip.Entries[j]
		if previousOwner != nil && *previousOwner == r.Peername && theirs.Peer != r.Peername {
			return ErrEntryInMyRange
		}
		addToResult(*theirs)
		previousOwner = nil
	}

	r.Entries = result
	return nil
}

// UpdateRing updates the ring with the state from another ring
func (r *Ring) UpdateRing(gossipedRing GossipState) error {
	skew := now() - gossipedRing.Now
	if -maxClockSkew > skew || skew > maxClockSkew {
		return ErrClockSkew
	}

	if err := r.merge(*gossipedRing); err != nil {
		return err
	}
	return nil
}

type GossipState *Ring

// GossipState returns the state of the ring to be encoded as gossip
func (r *Ring) GossipState() GossipState {
	return r
}

// Empty returns true if the ring has no entries
func (r *Ring) Empty() bool {
	return len(r.Entries) == 0
}

// Given a slice of ranges which are all in the right order except
// possibly the last one spans zero, fix that up and return the slice
func (r *Ring) splitRangesOverZero(ranges []utils.Range) []utils.Range {
	if len(ranges) == 0 {
		return ranges
	}
	lastRange := ranges[len(ranges)-1]
	// if end token == start (ie last) entry on ring, we want to actually use r.End
	if lastRange.End == r.Start {
		ranges[len(ranges)-1].End = r.End
	} else if lastRange.End <= lastRange.Start {
		// We wrapped; want to split around 0
		// First shuffle everything up as we want results to be sorted
		ranges = append(ranges, utils.Range{})
		copy(ranges[1:], ranges[:len(ranges)-1])
		ranges[0] = utils.Range{Start: r.Start, End: lastRange.End}
		ranges[len(ranges)-1].End = r.End
	}
	return ranges
}

// OwnedRanges returns slice of Ranges, ordered by IP, indicating which
// ranges are owned by this peer.  Will split ranges which
// span 0 in the ring.
func (r *Ring) OwnedRanges() (result []utils.Range) {
	r.assertInvariants()

	for i, entry := range r.Entries {
		if entry.Peer == r.Peername {
			nextEntry := r.Entries.entry(i + 1)
			result = append(result, utils.Range{Start: entry.Token, End: nextEntry.Token})
		}
	}

	return r.splitRangesOverZero(result)
}

// ClaimForPeers claims the entire ring for the array of peers passed
// in.  Only works for empty rings.
func (r *Ring) ClaimForPeers(peers []router.PeerName) {
	utils.Assert(r.Empty())
	defer r.assertInvariants()
	defer r.updateExportedVariables()

	totalSize := r.distance(r.Start, r.End)
	share := totalSize/utils.Offset(len(peers)) + 1
	remainder := totalSize % utils.Offset(len(peers))
	pos := r.Start

	for i, peer := range peers {
		if utils.Offset(i) == remainder {
			share--
			if share == 0 {
				break
			}
		}

		if e, found := r.Entries.get(pos); found {
			e.update(peer, share)
		} else {
			r.Entries.insert(entry{Token: pos, Peer: peer, Free: share})
		}

		pos += utils.Address(share)
	}

	utils.Assert(pos == r.End)
}

func (r *Ring) ClaimItAll() {
	r.ClaimForPeers([]router.PeerName{r.Peername})
}

func (r *Ring) FprintWithNicknames(w io.Writer, m map[router.PeerName]string) {
	fmt.Fprintf(w, "Ring [%s, %s)\n", utils.AddressIP4(r.Start), utils.AddressIP4(r.End))
	for _, entry := range r.Entries {
		nickname, found := m[entry.Peer]
		if found {
			nickname = fmt.Sprintf(" (%s)", nickname)
		}

		fmt.Fprintf(w, "  %s -> %s%s (version: %d, free: %d)\n", utils.AddressIP4(entry.Token),
			entry.Peer, nickname, entry.Version, entry.Free)
	}
}

func (r *Ring) String() string {
	var buffer bytes.Buffer
	r.FprintWithNicknames(&buffer, make(map[router.PeerName]string))
	return buffer.String()
}

// ReportFree is used by the allocator to tell the ring
// how many free ips are in a given range, so that ChoosePeerToAskForSpace
// can make more intelligent decisions.
func (r *Ring) ReportFree(freespace map[utils.Address]utils.Offset) {
	r.assertInvariants()
	defer r.assertInvariants()
	defer r.updateExportedVariables()

	utils.Assert(!r.Empty())
	entries := r.Entries

	// As OwnedRanges splits around the origin, we need to
	// detect that here and fix up freespace
	if free, found := freespace[r.Start]; found && entries.entry(0).Token != r.Start {
		lastToken := entries.entry(-1).Token
		prevFree, found := freespace[lastToken]
		utils.Assert(found)
		freespace[lastToken] = prevFree + free
		delete(freespace, r.Start)
	}

	for start, free := range freespace {
		// Look for entry
		i := sort.Search(len(entries), func(j int) bool {
			return entries[j].Token >= start
		})

		// Are you trying to report free on space I don't own?
		utils.Assert(i < len(entries) && entries[i].Token == start && entries[i].Peer == r.Peername)

		// Check we're not reporting more space than the range
		entry, next := entries.entry(i), entries.entry(i+1)
		maxSize := r.distance(entry.Token, next.Token)
		utils.Assert(free <= maxSize)

		if entries[i].Free == free {
			return
		}

		entries[i].Free = free
		entries[i].Version++
	}
}

// ChoosePeerToAskForSpace chooses a weighted-random peer to ask
// for space.
func (r *Ring) ChoosePeerToAskForSpace() (result router.PeerName, err error) {
	var (
		sum               utils.Offset
		totalSpacePerPeer = make(map[router.PeerName]utils.Offset) // Compute total free space per peer
	)

	// iterate through tokens
	for _, entry := range r.Entries {
		// Ignore ranges with no free space
		if entry.Free <= 0 {
			continue
		}

		// Don't talk to yourself
		if entry.Peer == r.Peername {
			continue
		}

		totalSpacePerPeer[entry.Peer] += entry.Free
		sum += entry.Free
	}

	if sum == 0 {
		err = ErrNoFreeSpace
		return
	}

	// Pick random peer, weighted by total free space
	rn := rand.Int63n(int64(sum))
	for peername, space := range totalSpacePerPeer {
		rn -= int64(space)
		if rn < 0 {
			return peername, nil
		}
	}

	panic("Should never reach this point")
}

func (r *Ring) PickPeerForTransfer() router.PeerName {
	for _, entry := range r.Entries {
		if entry.Peer != r.Peername {
			return entry.Peer
		}
	}
	return router.UnknownPeerName
}

// Transfer will mark all entries associated with 'from' peer as owned by 'to' peer
// and return ranges indicating the new space we picked up
func (r *Ring) Transfer(from, to router.PeerName) (error, []utils.Range) {
	r.assertInvariants()
	defer r.assertInvariants()
	defer r.updateExportedVariables()

	var newRanges []utils.Range
	found := false

	for i, entry := range r.Entries {
		if entry.Peer == from {
			found = true
			entry.Peer = to
			entry.Version++
			newRanges = append(newRanges, utils.Range{Start: entry.Token, End: r.Entries.entry(i + 1).Token})
		}
	}

	if !found {
		return ErrNotFound, nil
	}

	return nil, r.splitRangesOverZero(newRanges)
}

// Contains returns true if addr is in this ring
func (r *Ring) Contains(addr utils.Address) bool {
	return addr >= r.Start && addr < r.End
}

// Owner returns the peername which owns the range containing addr
func (r *Ring) Owner(token utils.Address) router.PeerName {
	utils.Assert(r.Start <= token && token < r.End)

	r.assertInvariants()
	// There can be no owners on an empty ring
	if r.Empty() {
		return router.UnknownPeerName
	}

	// Look for the right-most entry, less than or equal to token
	preceedingEntry := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token > token
	})
	preceedingEntry--
	entry := r.Entries.entry(preceedingEntry)
	return entry.Peer
}
