package router

import (
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"
)

func NewRemoteConnection(from, to *Peer, tcpAddr string) *RemoteConnection {
	return &RemoteConnection{
		local:         from,
		remote:        to,
		remoteTCPAddr: tcpAddr}
}

func (conn *RemoteConnection) Local() *Peer {
	return conn.local
}

func (conn *RemoteConnection) Remote() *Peer {
	return conn.remote
}

func (conn *RemoteConnection) RemoteTCPAddr() string {
	return conn.remoteTCPAddr
}

func (conn *RemoteConnection) Shutdown() {
}

func (conn *RemoteConnection) Established() bool {
	return true
}

func (conn *RemoteConnection) String() string {
	from := "<nil>"
	if conn.local != nil {
		from = conn.local.Name.String()
	}
	to := "<nil>"
	if conn.remote != nil {
		to = conn.remote.Name.String()
	}
	return fmt.Sprint("Connection ", from, "->", to)
}

// Async. Does not return anything. If the connection is successful,
// it will end up in the local peer's connections map.
func NewLocalConnection(connRemote *RemoteConnection, acceptNewPeer bool, tcpConn *net.TCPConn, udpAddr *net.UDPAddr, router *Router) {
	if connRemote.local != router.Ourself.Peer {
		log.Fatal("Attempt to create local connection from a peer which is not ourself")
	}

	queryChan := make(chan *ConnectionInteraction, ChannelSize)
	// NB, we're taking a copy of connRemote here.
	connLocal := &LocalConnection{
		RemoteConnection: *connRemote,
		Router:           router,
		TCPConn:          tcpConn,
		remoteUDPAddr:    udpAddr,
		effectivePMTU:    DefaultPMTU,
		queryChan:        queryChan}
	go connLocal.queryLoop(queryChan, acceptNewPeer)
}

func (conn *LocalConnection) Established() bool {
	conn.RLock()
	defer conn.RUnlock()
	return conn.established
}

// Read by the forwarder processes when in the UDP senders
func (conn *LocalConnection) RemoteUDPAddr() *net.UDPAddr {
	conn.RLock()
	defer conn.RUnlock()
	return conn.remoteUDPAddr
}

// Called by the forwarder processes in a few places (including
// crypto), but the connection TCP receiver process, and by the local
// peer actor process. Do not call this from the connection's actor
// process itself.
func (conn *LocalConnection) CheckFatal(err error) error {
	if err == nil {
		return nil
	}
	conn.log("error:", err)
	conn.Shutdown()
	return err
}

// Called by forwarder processes, read in Forward (by sniffer and udp
// listener process in router).
func (conn *LocalConnection) setEffectivePMTU(pmtu int) {
	conn.Lock()
	defer conn.Unlock()
	if conn.effectivePMTU != pmtu {
		conn.effectivePMTU = pmtu
		conn.log("Effective PMTU set to", pmtu)
	}
}

// Called by the connection's actor process, and by the connection's
// TCP received process. StackFrag is read in conn.Forward (called by
// router udp listener and sniffer processes)
func (conn *LocalConnection) setStackFrag(frag bool) {
	conn.Lock()
	defer conn.Unlock()
	conn.stackFrag = frag
}

func (conn *LocalConnection) log(args ...interface{}) {
	v := append([]interface{}{}, fmt.Sprintf("->[%s]:", conn.remote.Name))
	v = append(v, args...)
	log.Println(v...)
}

// ACTOR client API

const (
	CSendTCP          = iota
	CSetEstablished   = iota
	CSetRemoteUDPAddr = iota
	CShutdown         = iota
)

// Async
func (conn *LocalConnection) Shutdown() {
	conn.queryChan <- &ConnectionInteraction{Interaction: Interaction{code: CShutdown}}
}

// Async
func (conn *LocalConnection) SetRemoteUDPAddr(remoteUDPAddr *net.UDPAddr) {
	if remoteUDPAddr == nil {
		return
	}
	conn.queryChan <- &ConnectionInteraction{
		Interaction: Interaction{code: CSetRemoteUDPAddr},
		payload:     remoteUDPAddr}
}

// Async
func (conn *LocalConnection) SetEstablished() {
	conn.queryChan <- &ConnectionInteraction{Interaction: Interaction{code: CSetEstablished}}
}

// Async
func (conn *LocalConnection) SendTCP(msg []byte) {
	conn.queryChan <- &ConnectionInteraction{
		Interaction: Interaction{code: CSendTCP},
		payload:     msg}
}

// ACTOR server

func (conn *LocalConnection) queryLoop(queryChan <-chan *ConnectionInteraction, acceptNewPeer bool) {
	err := conn.handshake(acceptNewPeer)
	if err != nil {
		log.Printf("->[%s] encountered error during handshake: %v\n", conn.remoteTCPAddr, err)
		conn.handleShutdown()
		return
	}
	conn.Router.Ourself.AddConnection(conn)
	if conn.remoteUDPAddr != nil {
		if err = conn.ensureForwarders(); err == nil {
			conn.heartbeat = time.NewTicker(FastHeartbeat)
			conn.forwardHeartbeatFrame() // avoid initial wait
		}
	}
	terminate := false
	for !terminate {
		if err != nil {
			conn.log("error:", err)
			break
		}
		select {
		case query, ok := <-queryChan:
			if !ok {
				break
			}
			switch query.code {
			case CShutdown:
				terminate = true
			case CSetEstablished:
				err = conn.handleSetEstablished()
			case CSetRemoteUDPAddr:
				err = conn.handleSetRemoteUDPAddr(query.payload.(*net.UDPAddr))
			case CSendTCP:
				err = conn.handleSendTCP(query.payload.([]byte))
			}
		case <-tickerChan(conn.heartbeat):
			conn.forwardHeartbeatFrame()
		case <-tickerChan(conn.fetchAll):
			err = conn.handleSendTCP(ProtocolFetchAllByte)
		case <-tickerChan(conn.fragTest):
			conn.setStackFrag(false)
			err = conn.handleSendTCP(ProtocolStartFragmentationTestByte)

		}
	}
	conn.handleShutdown()
}

func (conn *LocalConnection) handleSetRemoteUDPAddr(remoteUDPAddr *net.UDPAddr) error {
	if remoteUDPAddr == nil {
		return nil
	}
	conn.Lock()
	old := conn.remoteUDPAddr
	conn.remoteUDPAddr = remoteUDPAddr
	conn.Unlock()
	if old == nil {
		if err := conn.handleSendTCP(ProtocolConnectionEstablishedByte); err != nil {
			return err
		}
		return conn.handleSetEstablished()
	} else if old.String() != remoteUDPAddr.String() {
		log.Println("Peer", conn.remote.Name, "moved from", old, "to", remoteUDPAddr)
	}
	return nil
}

func (conn *LocalConnection) handleSetEstablished() error {
	conn.Lock()
	old := conn.established
	conn.established = true
	conn.Unlock()
	if !old {
		conn.Router.Ourself.ConnectionEstablished(conn)
		if err := conn.ensureForwarders(); err != nil {
			return err
		}
		stopTicker(conn.heartbeat)
		conn.heartbeat = time.NewTicker(SlowHeartbeat)
		conn.fetchAll = time.NewTicker(FetchAllInterval)
		conn.fragTest = time.NewTicker(FragTestInterval)
		conn.forwardHeartbeatFrame() // avoid initial wait
		// Send a large frame down the DF channel in order to prompt
		// PMTU discovery to start.
		conn.Forward(true, &ForwardedFrame{
			srcPeer: conn.local,
			dstPeer: conn.remote,
			frame:   PMTUDiscovery},
			nil)
		conn.setStackFrag(false)
		return conn.handleSendTCP(ProtocolStartFragmentationTestByte)
	}
	return nil
}

func (conn *LocalConnection) handleSendTCP(msg []byte) error {
	return conn.tcpSender.Send(msg)
}

func (conn *LocalConnection) handleShutdown() {
	if conn.remote != nil {
		conn.log("connection shutting down")
	}

	// Whilst some of these elements may have been written to whilst
	// holding locks, they were only written to by the connection
	// actor process. handleShutdown is only called by the connection
	// actor process. So there is no need to take locks to read these
	// (or write elements which are only read by the same
	// process). Taking locks is only done for elements which are read
	// by other processes.
	if conn.shutdown {
		return
	}
	conn.shutdown = true

	if conn.TCPConn != nil {
		checkWarn(conn.TCPConn.Close())
	}

	if conn.remote != nil {
		conn.remote.DecrementLocalRefCount()
		conn.Router.Ourself.DeleteConnection(conn)
	}

	stopTicker(conn.heartbeat)
	stopTicker(conn.fetchAll)
	stopTicker(conn.fragTest)

	// blank out the forwardChan so that the router processes don't
	// try to send any more
	conn.stopForwarders()

	conn.Router.ConnectionMaker.ConnectionTerminated(conn.remoteTCPAddr)
}

func (conn *LocalConnection) handshake(acceptNewPeer bool) error {
	// We do not need to worry about locking in here as at this point,
	// the connection is not reachable by any go-routine other than
	// ourself. Only when we add this connection to the conn.local
	// peer will it be visible from multiple go-routines.
	tcpConn := conn.TCPConn
	tcpConn.SetLinger(0)
	conn.extendReadDeadline()

	enc := gob.NewEncoder(tcpConn)

	localConnID := randUint64()

	versionStr := fmt.Sprint(ProtocolVersion)
	handshakeSend := map[string]string{
		"Protocol":        Protocol,
		"ProtocolVersion": versionStr,
		"PeerNameFlavour": PeerNameFlavour,
		"Name":            conn.local.Name.String(),
		"UID":             fmt.Sprint(conn.local.UID),
		"ConnID":          fmt.Sprint(localConnID)}
	handshakeRecv := map[string]string{}

	usingPassword := conn.Router.UsingPassword()
	var public, private *[32]byte
	var err error
	if usingPassword {
		public, private, err = GenerateKeyPair()
		if err != nil {
			return err
		}
		handshakeSend["PublicKey"] = hex.EncodeToString(public[:])
	}
	enc.Encode(handshakeSend)

	dec := gob.NewDecoder(tcpConn)
	err = dec.Decode(&handshakeRecv)
	if err != nil {
		return err
	}
	_, err = checkHandshakeStringField("Protocol", Protocol, handshakeRecv)
	if err != nil {
		return err
	}
	_, err = checkHandshakeStringField("ProtocolVersion", versionStr, handshakeRecv)
	if err != nil {
		return err
	}
	_, err = checkHandshakeStringField("PeerNameFlavour", PeerNameFlavour, handshakeRecv)
	if err != nil {
		return err
	}
	nameStr, err := checkHandshakeStringField("Name", "", handshakeRecv)
	if err != nil {
		return err
	}
	name, err := PeerNameFromString(nameStr)
	if err != nil {
		return err
	}
	if !acceptNewPeer {
		if _, found := conn.Router.Peers.Fetch(name); !found {
			return fmt.Errorf("Found unknown remote name: %s at %s", name, conn.remoteTCPAddr)
		}
	}
	if existingConn, found := conn.local.ConnectionTo(name); found && existingConn.Established() {
		return fmt.Errorf("Already have connection to %s at %s", name, existingConn.RemoteTCPAddr())
	}

	uidStr, err := checkHandshakeStringField("UID", "", handshakeRecv)
	if err != nil {
		return err
	}
	uid, err := strconv.ParseUint(uidStr, 10, 64)
	if err != nil {
		return err
	}

	remoteConnIdStr, err := checkHandshakeStringField("ConnID", "", handshakeRecv)
	if err != nil {
		return err
	}
	remoteConnID, err := strconv.ParseUint(remoteConnIdStr, 10, 64)
	if err != nil {
		return err
	}
	conn.UID = localConnID ^ remoteConnID

	if usingPassword {
		remotePublicStr, rpErr := checkHandshakeStringField("PublicKey", "", handshakeRecv)
		if rpErr != nil {
			return rpErr
		}
		remotePublicSlice, rpErr := hex.DecodeString(remotePublicStr)
		if rpErr != nil {
			return rpErr
		}
		remotePublic := [32]byte{}
		for idx, elem := range remotePublicSlice {
			remotePublic[idx] = elem
		}
		conn.SessionKey = FormSessionKey(&remotePublic, private, conn.Router.Password)
		conn.tcpSender = NewEncryptedTCPSender(enc, conn)
		conn.Decryptor = NewNaClDecryptor(conn)
	} else {
		if _, found := handshakeRecv["PublicKey"]; found {
			return fmt.Errorf("Remote network is encrypted. Password required.")
		}
		conn.tcpSender = NewSimpleTCPSender(enc)
		conn.Decryptor = NewNonDecryptor(conn)
	}

	toPeer := NewPeer(name, uid, 0)
	toPeer = conn.Router.Peers.FetchWithDefault(toPeer)
	if toPeer == nil {
		return fmt.Errorf("Connection appears to be with different version of a peer we already know of")
	} else if toPeer == conn.local {
		// have to do assigment here to ensure Shutdown releases ref count
		conn.remote = toPeer
		return fmt.Errorf("Cannot connect to ourself")
	}
	conn.remote = toPeer

	go conn.receiveTCP(dec, usingPassword)
	return nil
}

func checkHandshakeStringField(fieldName string, expectedValue string, handshake map[string]string) (string, error) {
	val, found := handshake[fieldName]
	if !found {
		return "", fmt.Errorf("Field %s is missing", fieldName)
	}
	if expectedValue != "" && val != expectedValue {
		return "", fmt.Errorf("Field %s has wrong value; expected '%s', received '%s'", fieldName, expectedValue, val)
	}
	return val, nil
}

func (conn *LocalConnection) receiveTCP(decoder *gob.Decoder, usingPassword bool) {
	defer conn.Decryptor.Shutdown()
	var receiver TCPReceiver
	if usingPassword {
		receiver = NewEncryptedTCPReceiver(conn)
	} else {
		receiver = NewSimpleTCPReceiver()
	}
	var err error
	for {
		var msg []byte
		conn.extendReadDeadline()
		if conn.CheckFatal(decoder.Decode(&msg)) != nil {
			return
		}
		msg, err = receiver.Decode(msg)
		if err != nil {
			checkWarn(err)
			// remote peer may be using wrong password. Or there could
			// be some sort of injection attack going on. Just ignore
			// the traffic rather than shutting down.
			continue
		}
		if msg[0] == ProtocolConnectionEstablished {
			// We initiated the connection. We sent fast heartbeats to
			// the remote side, which has now received at least one of
			// them and thus has informed us via TCP that it considers
			// the connection is now up. We now do a fetchAll on it.
			conn.SetEstablished()
			conn.SendTCP(ProtocolFetchAllByte)
		} else if msg[0] == ProtocolStartFragmentationTest {
			conn.Forward(false, &ForwardedFrame{
				srcPeer: conn.local,
				dstPeer: conn.remote,
				frame:   FragTest},
				nil)
		} else if msg[0] == ProtocolFragmentationReceived {
			conn.setStackFrag(true)
		} else if usingPassword && msg[0] == ProtocolNonce {
			conn.Decryptor.ReceiveNonce(msg[1:])
		} else if msg[0] == ProtocolFetchAll {
			// There are exactly two messages that relate to topology
			// updates.
			//
			// 1. FetchAll. This carries no payload. The receiver
			// responds with the entire topology model as the receiver
			// has it.
			//
			// 2. Update. This carries a topology payload. The
			// receiver merges it with its own topology model. If the
			// payload is a subset of the receiver's topology, no
			// further action is taken. Otherwise, the receiver sends
			// out to all its connections an "improved" update:
			//  - elements which the original payload added to the
			//    receiver are included
			//  - elements which the original payload updated in the
			//    receiver are included
			//  - elements which are equal between the receiver and
			//    the payload are not included
			//  - elements where the payload was older than the
			//    receiver's version are updated
			conn.SendTCP(Concat(ProtocolUpdateByte, conn.Router.Peers.EncodeAllPeers()))
		} else if msg[0] == ProtocolUpdate {
			newUpdate, err := conn.Router.Peers.ApplyUpdate(msg[1:])
			if _, ok := err.(UnknownPeersError); err != nil && ok {
				// That update contained a peer we didn't know about;
				// request full update
				conn.SendTCP(ProtocolFetchAllByte)
				continue
			}
			if conn.CheckFatal(err) != nil {
				return
			}
			if len(newUpdate) != 0 {
				conn.Router.ConnectionMaker.Refresh()
				conn.Router.Routes.Recalculate()
				conn.Router.Ourself.BroadcastTCP(Concat(ProtocolUpdateByte, newUpdate))
			}
		} else if msg[0] == ProtocolPMTUVerified {
			conn.verifyPMTU <- int(binary.BigEndian.Uint16(msg[1:]))
		} else if msg[0] == ProtocolGossipUnicast {
			origMsg := msg
			channelHash, msg := decodeGossipChannel(msg[1:])
			srcName, _, msg := decodePeerName(msg)
			destName, _, msg := decodePeerName(msg)
			if conn.local.Name == destName {
				channel, found := conn.Router.GossipChannels[channelHash]
				if !found {
					conn.log("received unknown gossip channel:\n", channelHash)
				} else {
					channel.gossiper.OnGossipUnicast(srcName, msg)
				}
			} else {
				conn.local.RelayGossipTo(destName, origMsg)
			}
		} else if msg[0] == ProtocolGossipBroadcast {
			// intended for state from sending peer only
			// done when there is a change that everyone should hear about quickly
			// relayed using broadcast topology.
			origMsg := msg
			channelHash, msg := decodeGossipChannel(msg[1:])
			srcName, _, msg := decodePeerName(msg)
			channel, found := conn.Router.GossipChannels[channelHash]
			if !found {
				conn.log("received unknown gossip channel:\n", channelHash)
			} else {
				channel.gossiper.OnGossipBroadcast(msg)
			}
			conn.local.RelayGossipBroadcast(srcName, origMsg)
		} else if msg[0] == ProtocolGossip {
			// contains state for everyone that sending peer knows
			// peers that receive it should examine the info, and if any of it is newer then
			// pass it on to their peers
			channelHash, msg := decodeGossipChannel(msg[1:])
			channel, found := conn.Router.GossipChannels[channelHash]
			if !found {
				conn.log("received unknown gossip channel:\n", channelHash)
			} else {
				_, _, msg := decodePeerName(msg)
				newBuf := channel.gossiper.OnGossip(msg)
				if newBuf != nil {
					channel.GossipMsg(newBuf)
				}
			}
		} else {
			conn.log("received unknown msg:\n", msg)
		}
	}
}

func (conn *LocalConnection) extendReadDeadline() {
	conn.TCPConn.SetReadDeadline(time.Now().Add(ReadTimeout))
}

// Heartbeating serves two purposes: a) keeping NAT paths alive, and
// b) updating a remote peer's knowledge of our address, in the event
// it changes (e.g. because NAT paths expired).
// Called only by connection actor process.
func (conn *LocalConnection) forwardHeartbeatFrame() {
	heartbeatFrame := &ForwardedFrame{
		srcPeer: conn.local,
		dstPeer: conn.remote,
		frame:   []byte{}}
	conn.Forward(true, heartbeatFrame, nil)
}

func tickerChan(ticker *time.Ticker) <-chan time.Time {
	if ticker != nil {
		return ticker.C
	}
	return nil
}

func stopTicker(ticker *time.Ticker) {
	if ticker != nil {
		ticker.Stop()
	}
}
