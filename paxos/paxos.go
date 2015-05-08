package paxos

import (
	"bytes"
	"encoding/gob"
	//"fmt"
	"github.com/weaveworks/weave/router"
)

// note all fields exported in structs so we can Gob them
type ProposalID struct {
	// round numbers begin at 1.  round 0 indicates an
	// uninitialized ProposalID, and precedes all other ProposalIDs
	Round    uint
	Proposer router.PeerName
}

func (a ProposalID) equals(b ProposalID) bool {
	return a.Round == b.Round && a.Proposer == b.Proposer
}

func (a ProposalID) precedes(b ProposalID) bool {
	return a.Round < b.Round || (a.Round == b.Round && a.Proposer < b.Proposer)
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
	return a.Promise.equals(b.Promise) && a.Accepted.equals(b.Accepted) &&
		a.AcceptedVal.Origin.equals(b.AcceptedVal.Origin)
}

type Node struct {
	actionChan chan<- func()
	gossip     router.Gossip // our link to the outside world for sending messages
	id         router.PeerName
	quorum     uint
	knows      map[router.PeerName]NodeClaims
	// The first consensus the Node observed
	firstConsensus AcceptedValue
}

func (node *Node) Init(id router.PeerName, quorum uint) {
	node.id = id
	node.quorum = quorum
	node.knows = map[router.PeerName]NodeClaims{}
}

func (node *Node) encode() []byte {
	//fmt.Printf("%d: encoding %x\n", node.id, node.knows)
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err := enc.Encode(node.knows); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func decodeNodeKnows(msg []byte) (map[router.PeerName]NodeClaims, error) {
	reader := bytes.NewReader(msg)
	decoder := gob.NewDecoder(reader)
	var knows map[router.PeerName]NodeClaims

	if err := decoder.Decode(&knows); err != nil {
		return nil, err
	}
	return knows, nil
}

// Update this node's information about what other nodes know.
// Returns true if we learned something new.
func (node *Node) update(msg []byte) bool {
	from_knows, _ := decodeNodeKnows(msg)
	//fmt.Printf("%d: decoded %x\n", node.id, from_knows)

	changed := false

	for i, from_claims := range from_knows {
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
func (node *Node) propose() {
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

// The heart of the consensus algorithm. Return true if we have changed our claims.
func (node *Node) think() bool {
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

	claims_changed := node.knows[node.id].equals(our_claims)
	node.knows[node.id] = our_claims

	if !node.firstConsensus.Origin.valid() {
		ok, val := node.consensus()
		if ok {
			//fmt.Printf("%d: we have consensus!\n", node.id)
			node.firstConsensus = val
		}
	}
	return claims_changed
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

func (node *Node) string() string {
	return "Paxos string todo"
}

func (node *Node) assertInvariants() {
	// todo
}

// paxosGossipData similar to ipamGossipData
type paxosGossipData struct {
	node *Node
}

func (d *paxosGossipData) Merge(other router.GossipData) {}
func (d *paxosGossipData) Encode() []byte {
	return d.node.Encode()
}

// SetInterfaces gives the allocator two interfaces for talking to the outside world
func (node *Node) SetInterfaces(gossip router.Gossip) {
	node.gossip = gossip
}

// Start runs the allocator goroutine
func (node *Node) Start(gossip router.Gossip) {
	node.gossip = gossip
	actionChan := make(chan func(), router.ChannelSize)
	node.actionChan = actionChan
	go node.actorLoop(actionChan)
}

// Actor client API

// Propose (Async)
func (node *Node) Propose() {
	node.actionChan <- func() {
		node.propose()
	}
}

// Consensus for public consumption - return the set, or nil if no consensus. Sync.
func (node *Node) Consensus() map[router.PeerName]struct{} {
	resultChan := make(chan map[router.PeerName]struct{})
	node.actionChan <- func() {
		_, val := node.consensus()
		resultChan <- val.Value
	}
	return <-resultChan
}

// Encode (Sync)
func (node *Node) Encode() []byte {
	resultChan := make(chan []byte)
	node.actionChan <- func() {
		resultChan <- node.encode()
	}
	return <-resultChan
}

// Sync.
func (node *Node) String() string {
	resultChan := make(chan string)
	node.actionChan <- func() {
		resultChan <- node.string()
	}
	return <-resultChan
}

func (node *Node) OnGossipUnicast(sender router.PeerName, msg []byte) error {
	// not expected
	return nil
}

func (node *Node) OnGossipBroadcast(msg []byte) (router.GossipData, error) {
	resultChan := make(chan error)
	node.actionChan <- func() {
		node.update(msg)
		resultChan <- nil
	}
	return &paxosGossipData{node}, <-resultChan
}

func (node *Node) OnGossip(msg []byte) (router.GossipData, error) {
	resultChan := make(chan error)
	node.actionChan <- func() {
		node.update(msg)
		resultChan <- nil
	}
	return &paxosGossipData{node}, <-resultChan
}

// ACTOR server

func (node *Node) actorLoop(actionChan <-chan func()) {
	for {
		action := <-actionChan
		if action == nil {
			break
		}
		action()
		if node.think() {
			// something changed - tell everyone else
			node.gossip.GossipBroadcast(&paxosGossipData{node})
		}
		node.assertInvariants()
	}
}
