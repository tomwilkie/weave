/*
Ring implements a simple ring CRDT.

TODO: merge consequtively owned ranges
TODO: implement tombstones
*/
package ring

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/zettio/weave/ipam/utils"
	"github.com/zettio/weave/router"
	"math/rand"
	"net"
	"sort"
)

// Represents entries around the ring
type entry struct {
	Token     uint32          // The start of this range
	Peer      router.PeerName // Who owns this range
	Tombstone uint32          // Timestamp when this entry was tombstone; 0 means live
	Version   uint32          // Version of this range
	Free      uint32          // Number of free IPs in this range
}

func (e1 *entry) Equal(e2 *entry) bool {
	return e1.Token == e2.Token && e1.Peer == e2.Peer &&
		e1.Tombstone == e2.Tombstone && e1.Version == e2.Version
}

// For compatibility with sort.Interface
type entries []*entry

func (es entries) Len() int           { return len(es) }
func (es entries) Less(i, j int) bool { return es[i].Token < es[j].Token }
func (es entries) Swap(i, j int)      { panic("Should never be swapping entries!") }

func (es entries) String() string {
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "[")
	for i, entry := range es {
		fmt.Fprintf(&buffer, "%+v", *entry)
		if i+1 < len(es) {
			fmt.Fprintf(&buffer, " ")
		}
	}
	fmt.Fprintf(&buffer, "]")
	return buffer.String()
}

// Represents the ring itself
type Ring struct {
	Start, End uint32          // [min, max) tokens in this ring.  Due to wrapping, min == max (effectively)
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
	ErrDifferentSubnets = errors.New("Cannot merge gossip for different subnet!")
	ErrNewerVersion     = errors.New("Received new version for entry I own!")
	ErrInvalidEntry     = errors.New("Received invalid state update!")
	ErrEntryInMyRange   = errors.New("Received new entry in my range!")
	ErrNoFreeSpace      = errors.New("No free space found!")
	ErrTooMuchFreeSpace = errors.New("Entry reporting too much free space!")
)

func (r *Ring) checkInvariants() error {
	if !sort.IsSorted(r.Entries) {
		return ErrNotSorted
	}

	// Check no token appears twice
	tokens := make(map[uint32]bool)
	for _, entry := range r.Entries {
		if _, ok := tokens[entry.Token]; ok {
			return ErrTokenRepeated
		}
		tokens[entry.Token] = true
	}

	if len(r.Entries) == 0 {
		return nil
	}

	// Check tokens are in range
	if r.entry(0).Token < r.Start {
		return ErrTokenOutOfRange
	}

	if r.entry(-1).Token >= r.End {
		return ErrTokenOutOfRange
	}

	// Check all the freespaces are in range
	for i, entry := range r.Entries {
		next := r.entry(i + 1)
		distance := r.distance(entry.Token, next.Token)

		if entry.Free > distance {
			return ErrTooMuchFreeSpace
		}
	}

	return nil
}

func (r *Ring) insertAt(i int, e entry) {
	r.assertInvariants()

	if i < len(r.Entries) && r.Entries[i].Token == e.Token {
		panic("Trying to insert an existing token!")
	}

	r.Entries = append(r.Entries, &entry{})
	copy(r.Entries[i+1:], r.Entries[i:])
	r.Entries[i] = &e

	r.assertInvariants()
}

// New creates an empty ring belonging to peer.
func New(startIP, endIP net.IP, peer router.PeerName) *Ring {
	start, end := utils.Ip4int(startIP), utils.Ip4int(endIP)
	utils.Assert(start <= end, "Start needs to be less than end!")

	return &Ring{start, end, peer, make([]*entry, 0)}
}

func (r *Ring) entry(i int) *entry {
	i = i % len(r.Entries)
	if i < 0 {
		i += len(r.Entries)
	}
	return r.Entries[i]
}

// Is token between entries at i and j?
// NB i and j can overflow and will wrap
// NBB if entries[i].token == token, this will return true
func (r *Ring) between(token uint32, i, j int) bool {
	utils.Assert(i < j, "Start and end must be in order")

	first := r.entry(i)
	second := r.entry(j)

	switch {
	case first.Token == second.Token:
		// This implies there is only one token
		// on the ring (i < j and i.token == j.token)
		// In which case everything is between, expect
		// this one token
		return token != first.Token

	case first.Token < second.Token:
		return first.Token <= token && token < second.Token

	case second.Token < first.Token:
		return first.Token <= token || token < second.Token
	}

	panic("Should never get here - switch covers all possibilities.")
}

func (r *Ring) distance(start, end uint32) uint32 {
	if end > start {
		return end - start
	} else {
		return (r.End - start) + (end - r.Start)
	}
}

// Grant range [start, end) to peer
// Note, due to wrapping, end can be less than start
// TODO grant should calulate free correctly
func (r *Ring) GrantRangeToHost(startIP, endIP net.IP, peer router.PeerName) {
	r.assertInvariants()

	start, end := utils.Ip4int(startIP), utils.Ip4int(endIP)
	utils.Assert(r.Start <= start && start < r.End, "Trying to grant range outside of subnet")
	utils.Assert(r.Start < end && end <= r.End, "Trying to grant range outside of subnet")
	utils.Assert(len(r.Entries) > 0, "Cannot grant if ring is empty!")

	// How much space will the new range have?
	var newFree = r.distance(start, end)
	utils.Assert(newFree > 0, "Cannot create zero-sized ranges")

	// Look for the start entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	// Is start already owned by us, in which case we need
	// to change the token and update version
	if i < len(r.Entries) && r.Entries[i].Token == start {
		entry := r.Entries[i]
		utils.Assert(entry.Peer == r.Peername, "Trying to mutate entry I don't own")
		entry.Peer = peer
		entry.Tombstone = 0
		entry.Version++
		entry.Free = newFree
	} else {
		// Otherwise, these isn't a token here, we need to
		// find the preceeding token and check we own it (being careful for wrapping)
		j := i - 1
		utils.Assert(r.between(start, j, i), "??")

		previous := r.entry(j)
		utils.Assert(previous.Peer == r.Peername, "Trying to mutate range I don't own")

		// Reset free on previous token; may over estimate, but thats fine
		previous.Free = r.distance(previous.Token, start)
		previous.Version++

		r.insertAt(i, entry{Token: start, Peer: peer, Free: newFree})
	}

	r.assertInvariants()

	// Now we need to deal with the end token.  There are 3 cases:
	//   i.   the next token is equal to the end of the range
	//        => we don't need to do anything
	//   ii.  the end is between this token and the next,
	//        => we need to insert a token such that
	//        we claim this bit on the end.
	//   iii. the end is not between this token and next
	//        => this is an error, we'll be splitting someone
	//        else's ranges.

	k := i + 1
	nextEntry := r.entry(k)

	// End needs wrapping
	end = r.Start + ((end - r.Start) % (r.End - r.Start))

	if nextEntry.Token == end {
		// That was easy
		return
	} else {
		utils.Assert(r.between(end, i, k), "End spans another token")
		distance := r.distance(end, r.entry(k).Token)
		r.insertAt(k, entry{Token: end, Peer: r.Peername, Free: distance})
		r.assertInvariants()
	}
}

// Merge the given ring into this ring.
func (r *Ring) merge(gossip Ring) error {
	r.assertInvariants()

	// Don't panic when checking the gossiped in ring.
	// In this case just return any error found.
	if err := gossip.checkInvariants(); err != nil {
		return err
	}

	if r.Start != gossip.Start || r.End != gossip.End {
		return ErrDifferentSubnets
	}

	// Special case gossip ring being empty - not much to do...
	if len(gossip.Entries) == 0 {
		return nil
	}

	// We special case us having an empty ring -
	// in this case we might be coming up in an existing
	// network and be given some ranges we might have forgotten
	// abouts.  Assertions below would fail as it would appear
	// other nodes are gossiping tokens in ranges we own.
	if len(r.Entries) == 0 {
		r.Entries = make([]*entry, len(gossip.Entries))
		for i := range r.Entries {
			r.Entries[i] = new(entry)
			*r.Entries[i] = *gossip.Entries[i]
		}
		return nil
	}

	// First count number of distinct tokens to build the result with
	tokens := make(map[uint32]bool)
	for _, entry := range r.Entries {
		tokens[entry.Token] = true
	}
	for _, entry := range gossip.Entries {
		tokens[entry.Token] = true
	}

	// mergeEntry merges two entries with the same token
	// returns the merged entry, flag indicating if this was
	// from the gossiped ring, and an error
	mergeEntry := func(existingEntry, newEntry entry) (entry, bool, error) {
		utils.Assert(existingEntry.Token == newEntry.Token, "WTF")
		switch {
		case existingEntry.Version == newEntry.Version:
			if !existingEntry.Equal(&newEntry) {
				return entry{}, false, ErrInvalidEntry
			}
			return existingEntry, false, nil

		case existingEntry.Version < newEntry.Version:
			// A new token it getting inserted
			if existingEntry.Peer == r.Peername {
				return entry{}, false, ErrNewerVersion
			}
			return newEntry, true, nil

		case existingEntry.Version > newEntry.Version:
			return existingEntry, false, nil
		}

		panic("Should never get here - switch covers all possibilities.")
	}

	// Make new slice for result; iterate over
	// existing state and new state merging into
	// result and checking for invariants.
	result := make([]entry, len(tokens))
	i, j := 0, 0

	// Owner is the owner of the largest token
	// We know are ring isn't empty at this point.
	var currentOwner router.PeerName
	last := func(es []*entry) *entry { return es[len(es)-1] }
	if last(r.Entries).Token > last(gossip.Entries).Token {
		currentOwner = last(r.Entries).Peer
	} else {
		currentOwner = last(gossip.Entries).Peer
	}

	// Consumes an entry from the gossiped ring
	// and puts it in out ring at k
	consumeGossip := func(k int) error {
		if currentOwner == r.Peername {
			return ErrEntryInMyRange
		}
		// Don't copy the pointer, copy the real entry!
		result[k] = *gossip.Entries[j]
		j++

		// Don't update current owner if we've consumed a
		// gossip entry, as we might have been given some
		// space, and this would cause us to reject the
		// next token
		currentOwner = router.UnknownPeerName
		return nil
	}

	// Consumes an entry from our local state
	consumeLocal := func(k int) {
		result[k] = *r.Entries[i]
		i++
		currentOwner = result[k].Peer
	}

	// Merges an entry from the gossiped ring with our state
	consumeBoth := func(k int) error {
		entry, remote, err := mergeEntry(*r.Entries[i], *gossip.Entries[j])
		if err != nil {
			return err
		} else {
			result[k] = entry
		}
		i++
		j++

		// Don't update current owner if we've consumed a
		// gossip entry, as we might have been given some
		// space, and this would cause us to reject the
		// next token
		if !remote {
			currentOwner = result[k].Peer
		} else {
			currentOwner = router.UnknownPeerName
		}
		return nil
	}

	for k := range result {
		// Ordering of cases is important here!
		switch {
		case i >= len(r.Entries):
			if err := consumeGossip(k); err != nil {
				return err
			}

		case j >= len(gossip.Entries):
			consumeLocal(k)

		case r.Entries[i].Token > gossip.Entries[j].Token:
			if err := consumeGossip(k); err != nil {
				return err
			}

		case r.Entries[i].Token < gossip.Entries[j].Token:
			consumeLocal(k)

		case r.Entries[i].Token == gossip.Entries[j].Token:
			if err := consumeBoth(k); err != nil {
				return err
			}
		}
	}
	utils.Assert(i == len(r.Entries) && j == len(gossip.Entries), "WTF")

	r.Entries = make([]*entry, len(tokens))
	for k := range result {
		r.Entries[k] = &result[k]
	}
	r.assertInvariants()
	return nil
}

func (r *Ring) OnGossipBroadcast(msg []byte) error {
	reader := bytes.NewReader(msg)
	decoder := gob.NewDecoder(reader)
	gossipedRing := Ring{}

	if err := decoder.Decode(&gossipedRing); err != nil {
		return err
	}

	if err := r.merge(gossipedRing); err != nil {
		return err
	}
	return nil
}

func (r *Ring) GossipState() []byte {
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(r); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (r *Ring) Empty() bool {
	return len(r.Entries) == 0
}

// Return type for OwnedRanges.
// NB End can be less than Start for ranges
// which cross 0.
type Range struct {
	Start, End net.IP // [Start, End) of range I own
}

type RangeSlice []Range

// Return slice of Ranges indicating which
// ranges are owned by this peer.
func (r *Ring) OwnedRanges() RangeSlice {
	// Cannot return more the entries + 1 results
	// +1 for splitting results spanning 0
	result := make([]Range, len(r.Entries)+1)
	j := 0 // index into above slice

	// Finally iterate through entries again
	// filling in result as we go.
	for i, entry := range r.Entries {
		if entry.Peer != r.Peername {
			continue
		}

		// next logical token in ring; be careful of
		// wrapping the index
		nextEntry := r.entry(i + 1)

		switch {
		case nextEntry.Token == r.Start:
			// be careful here; if end token == start (ie last)
			// entry on ring, we want to actually use r.End
			result[j] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(r.End)}
			j++

		case nextEntry.Token <= entry.Token:
			// We wrapped; want to split around 0
			// First shuffle everything up as we want results to be sorted
			copy(result[1:j+1], result[:j])
			result[0] = Range{Start: utils.Intip4(r.Start),
				End: utils.Intip4(nextEntry.Token)}
			result[j+1] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(r.End)}
			j = j + 2

		default:
			result[j] = Range{Start: utils.Intip4(entry.Token),
				End: utils.Intip4(nextEntry.Token)}
			j++
		}
	}

	return result[:j]
}

// Claim entire ring.  Only works for empty rings.
func (r *Ring) ClaimItAll() {
	utils.Assert(len(r.Entries) == 0, "Cannot bootstrap ring with entries in it!")

	// We reserve the first and last address with a special range; this ensures
	// they are never given out by anyone
	// Per RFC1122, don't allocate the first (network) and last (broadcast) addresses
	r.insertAt(0, entry{Token: r.Start + 1, Peer: r.Peername,
		Free: r.End - r.Start - 2})
	r.insertAt(1, entry{Token: r.End - 1, Peer: router.UnknownPeerName})

	r.assertInvariants()
}

func (r *Ring) String() string {
	var buffer bytes.Buffer
	for _, entry := range r.Entries {
		fmt.Fprintf(&buffer, "%s -> %s (%d, %d, %d)\n", utils.Intip4(entry.Token),
			entry.Peer, entry.Tombstone, entry.Version, entry.Free)
	}
	return buffer.String()
}

func (r *Ring) ReportFree(startIP net.IP, free uint32) {
	start := utils.Ip4int(startIP)

	// Look for entry
	i := sort.Search(len(r.Entries), func(j int) bool {
		return r.Entries[j].Token >= start
	})

	utils.Assert(i < len(r.Entries) && r.Entries[i].Token == start &&
		r.Entries[i].Peer == r.Peername, "Trying to report free on space I don't own")

	// Check we're not reporting more space than the range
	entry, next := r.entry(i), r.entry(i+1)
	maxSize := r.distance(entry.Token, next.Token)
	utils.Assert(free <= maxSize, "Trying to report more free space than possible")

	if r.Entries[i].Free == free {
		return
	}

	r.Entries[i].Free = free
	r.Entries[i].Version++
}

func (r *Ring) ChoosePeerToAskForSpace() (result router.PeerName, err error) {
	// Compute total free space per peer
	var sum uint32 = 0
	totalSpacePerPeer := make(map[router.PeerName]uint32)
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
