package paxos

import (
	"fmt"
	"github.com/weaveworks/weave/router"
	"math/rand"
	"testing"
	"time"
)

type Link struct {
	from *Node
	to   *Node

	// A link is considered ready if it is worthwhile gossipping
	// along it (i.e. unless we already gossipped along it and the
	// "from" node didn't change since then).  If this is <0, then
	// the link is not ready.  Otherwise, this gives the index in
	// the Model's readyLinks slice.
	ready int
}

type Model struct {
	quorum     uint
	nodes      []Node
	readyLinks []*Link
	// Topology
	links    map[*Node][]*Link
	isolated map[*Node]bool
}

func (m *Model) addLink(a, b *Node) {
	ab := Link{from: a, to: b, ready: len(m.readyLinks)}
	m.links[a] = append(m.links[a], &ab)
	ba := Link{from: b, to: a, ready: len(m.readyLinks) + 1}
	m.links[b] = append(m.links[b], &ba)
	m.readyLinks = append(m.readyLinks, &ab, &ba)
}

type TestParams struct {
	// Number of nodes
	nodeCount uint

	// Probability that two nodes are connected.
	connectedProb float32

	// Probability that some node will re-propose at each
	// step. Setting this too high makes it likely that we'll fail
	// to converge.
	reproposeProb float32

	// Probability that a some node will be isolated at each step.
	isolateProb float32
}

// Make a network of nodes with random topology
func makeRandomModel(params *TestParams, r *rand.Rand) *Model {
	m := Model{
		quorum:     params.nodeCount/2 + 1,
		nodes:      make([]Node, params.nodeCount),
		readyLinks: []*Link{},
		isolated:   make(map[*Node]bool),
		links:      make(map[*Node][]*Link),
	}

	for i := range m.nodes {
		m.nodes[i].Init(router.PeerName(i+1), m.quorum)
		m.nodes[i].Propose()
	}

	for i := 1; i < len(m.nodes); i++ {
		// was node i connected to the other nodes yet?
		connected := false

		for j := 0; j < i; j++ {
			if r.Float32() < params.connectedProb {
				connected = true
				m.addLink(&m.nodes[i], &m.nodes[j])
			}
		}

		if !connected {
			// node i must be connected into the graph
			// somewhere.  So if we didn't connect it
			// already, this is a last resort.
			m.addLink(&m.nodes[i], &m.nodes[r.Intn(i)])
		}
	}

	return &m
}

// Mark all the outgoing links from a node as ready
func (m *Model) nodeChanged(node *Node) {
	for _, l := range m.links[node] {
		if l.ready < 0 {
			l.ready = len(m.readyLinks)
			m.readyLinks = append(m.readyLinks, l)
		}
	}
}

// Mark a link as unready
func (m *Model) unreadyLink(link *Link) {
	i := link.ready
	m.readyLinks[i] = m.readyLinks[len(m.readyLinks)-1]
	m.readyLinks[i].ready = i
	m.readyLinks = m.readyLinks[:len(m.readyLinks)-1]
	link.ready = -1
}

// Isolate a node
func (m *Model) isolateNode(node *Node) {
	m.isolated[node] = true
	for _, l := range m.links[node] {
		if l.ready >= 0 {
			m.unreadyLink(l)
		}
	}
}

func (m *Model) pickNode(r *rand.Rand) *Node {
	for {
		node := &m.nodes[r.Intn(len(m.nodes))]
		if !m.isolated[node] {
			return node
		}
	}
}

func (m *Model) simulate(params *TestParams, r *rand.Rand) bool {
	nodesLeft := uint(len(m.nodes))

	for i := 0; i < 1000000; i++ {
		if len(m.readyLinks) == 0 {
			// Everything has converged
			return true
		}

		// Pick a ready link at random
		i := r.Intn(len(m.readyLinks))
		link := m.readyLinks[i]
		if link.ready != i {
			panic("Link in readyLinks was not ready")
		}

		// gossip across link
		if _, _, changed := link.to.OnGossipBroadcast(link.from.Encode()); changed {
			m.nodeChanged(link.to)
		}

		m.unreadyLink(link)

		// Re-propose?
		if r.Float32() < params.reproposeProb {
			node := m.pickNode(r)
			node.Propose()
			m.nodeChanged(node)
		}

		// Isolate?
		if nodesLeft > m.quorum && r.Float32() < params.isolateProb {
			m.isolateNode(m.pickNode(r))
			nodesLeft--

			// We isolated a node, so get another node to
			// re-propose.  In reality the lack of
			// consensus would be detected via a timeout
			node := m.pickNode(r)
			node.Propose()
			m.nodeChanged(node)
		}
	}

	return false
}

// Validate the final model state
func (m *Model) validate() {
	var origin ProposalID

	for i := range m.nodes {
		ok, val := m.nodes[i].Consensus()
		if !ok {
			panic("Node doesn't know about consensus")
		}

		if m.nodes[i].firstConsensus.Origin != val.Origin {
			panic("Consensus mismatch")
		}

		if i == 0 {
			origin = val.Origin
		} else if val.Origin != origin {
			panic("Node disagrees about consensus")
		}
	}
}

func TestPaxos(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	params := TestParams{
		nodeCount:     10,
		connectedProb: 0.5,
		reproposeProb: 0.01,
		isolateProb:   0.01,
	}

	for i := 0; i < 10; i++ {
		m := makeRandomModel(&params, r)

		if !m.simulate(&params, r) {
			panic("Failed to converge")
		}

		m.validate()
	}

	fmt.Println("Done")
}
