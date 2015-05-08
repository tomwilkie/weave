package paxos

import (
	"github.com/weaveworks/weave/router"
)

// note all fields exported in structs so we can Gob them
type ProposalID struct {
	// round numbers begin at 1.  round 0 indicates an
	// uninitialized ProposalID, and precedes all other ProposalIDs
	Round    uint
	Proposer router.PeerName
}

func (a ProposalID) precedes(b ProposalID) bool {
	return a.Round < b.Round ||
		(a.Round == b.Round && a.Proposer < b.Proposer)
}

func (a ProposalID) valid() bool {
	return a.Round > 0
}

// For seeding IPAM, the value we want consensus on is a set of nodes
type Value map[router.PeerName]struct{}

// An AcceptedValue is a Value plus the proposal which originated that
// Value.  The origin is not essential, but makes comparing
// AcceptedValues easy even if comparing Values is not.
type AcceptedValue struct {
	Value  Value
	Origin ProposalID
}

type NodeClaims struct {
	// The node promises not to accept a proposal with id less
	// than this.
	Promise ProposalID

	// The accepted proposal, if valid
	Accepted    ProposalID
	AcceptedVal AcceptedValue
}

func (a NodeClaims) equals(b NodeClaims) bool {
	return a.Promise == b.Promise && a.Accepted == b.Accepted &&
		a.AcceptedVal.Origin == b.AcceptedVal.Origin
}

type GossipState map[router.PeerName]NodeClaims

type Node struct {
	id     router.PeerName
	quorum uint
	knows  GossipState
}

func (node *Node) Init(id router.PeerName, quorum uint) {
	node.id = id
	node.quorum = quorum
	node.knows = map[router.PeerName]NodeClaims{}
}

func (node *Node) GossipState() GossipState {
	return node.knows
}

// Update this node's information about what other nodes know.
// Returns true if we learned something new.
func (node *Node) Update(from GossipState) bool {
	changed := false

	for i, from_claims := range from {
		claims, ok := node.knows[i]
		if ok {
			if claims.Promise.precedes(from_claims.Promise) {
				claims.Promise = from_claims.Promise
				changed = true
			}

			if claims.Accepted.precedes(from_claims.Accepted) {
				claims.Accepted = from_claims.Accepted
				claims.AcceptedVal = from_claims.AcceptedVal
				changed = true
			}
		} else {
			claims = from_claims
			changed = true
		}

		node.knows[i] = claims
	}
	return changed
}

func max(a uint, b uint) uint {
	if a > b {
		return a
	} else {
		return b
	}
}

// Initiate a new proposal, i.e. the Paxos "Prepare" step.  This is
// simply a matter of gossipping a new proposal that supersedes all
// others.
func (node *Node) Propose() {
	// Find the highest round number around
	round := uint(0)

	for _, claims := range node.knows {
		round = max(round, claims.Promise.Round)
		round = max(round, claims.Accepted.Round)
	}

	our_claims := node.knows[node.id]
	our_claims.Promise = ProposalID{
		Round:    round + 1,
		Proposer: node.id,
	}
	node.knows[node.id] = our_claims
}

// The heart of the consensus algorithm. Return true if we have
// changed our claims.
func (node *Node) Think() bool {
	our_claims := node.knows[node.id]

	// The "Promise" step of Paxos: Copy the highest known
	// promise.
	for _, claims := range node.knows {
		if our_claims.Promise.precedes(claims.Promise) {
			our_claims.Promise = claims.Promise
		}
	}

	// The "Accept Request" step of Paxos: Acting as a proposer,
	// do we have a proposal that has been promised by a quorum?
	//
	// In Paxos, the "proposer" and "acceptor" roles are distinct,
	// so in principle a node acting as a proposer could could
	// continue trying to get its proposal acccepted even after
	// the same node as an acceptor has superseded that proposal.
	// But that's pointless in a gossip context: If our promise
	// supersedes our own proposal, then anyone who hears about
	// that promise will not accept that proposal.  So our
	// proposal is only in the running if it is also our promise.
	if our_claims.Promise.Proposer == node.id {
		// Determine whether a quorum has promised, and the
		// best previously-accepted value if there is one.
		count := uint(0)
		var accepted ProposalID
		var acceptedVal AcceptedValue

		for _, claims := range node.knows {
			if claims.Promise == our_claims.Promise {
				count++

				if accepted.precedes(claims.Accepted) {
					accepted = claims.Accepted
					acceptedVal = claims.AcceptedVal
				}
			}
		}

		if count >= node.quorum {
			if !accepted.valid() {
				acceptedVal.Value = node.pickValue()
				acceptedVal.Origin = our_claims.Promise
			}

			// We automatically accept our own proposal,
			// and that's how we communicate the "accept
			// request" to other nodes.
			our_claims.Accepted = our_claims.Promise
			our_claims.AcceptedVal = acceptedVal
		}
	}

	// The "Accepted" step of Paxos: If the proposal we promised
	// on got accepted by some other node, we accept it too.
	for _, claims := range node.knows {
		if claims.Accepted == our_claims.Promise {
			our_claims.Accepted = claims.Accepted
			our_claims.AcceptedVal = claims.AcceptedVal
			break
		}
	}

	if our_claims.equals(node.knows[node.id]) {
		return false
	}

	node.knows[node.id] = our_claims
	return true
}

// When we get to pick a value, we use the set of nodes we know about.
// This is not necessarily all nodes, but it is at least a quorum, and
// so good enough for seeding the ring.
func (node *Node) pickValue() Value {
	val := Value{}

	for id := range node.knows {
		val[id] = struct{}{}
	}

	return val
}

// Has a consensus been reached, based on the known claims of other nodes?
func (node *Node) consensus() (bool, AcceptedValue) {
	counts := map[ProposalID]uint{}

	for _, claims := range node.knows {
		if claims.Accepted.valid() {
			origin := claims.AcceptedVal.Origin
			count := counts[origin] + 1
			counts[origin] = count
			if count >= node.quorum {
				return true, claims.AcceptedVal
			}
		}
	}

	return false, AcceptedValue{}
}

// Consensus for public consumption - return just the map, which will
// be nil if no consensus
func (node *Node) Consensus() map[router.PeerName]struct{} {
	_, val := node.consensus()
	return val.Value
}
