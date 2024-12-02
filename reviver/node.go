package main

import (
	"tp1-distribuidos/shared"
	"context"
	"encoding/csv"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type NodeState int

const (
	NodeStateFollower NodeState = iota
	NodeStateCandidate
	NodeStateWaitingCoordinator
	NodeStateCoordinator
)

type Node struct {
	name          string
	id            int
	peers         []*Peer
	serverConn    *net.TCPListener
	serverAddr    *net.TCPAddr
	stateLock     sync.Mutex
	state         NodeState
	currentLeader int
	stopContext   context.Context
	wg            sync.WaitGroup
	electionLock  *sync.Mutex
	stopLeader    chan struct{}
	processList   [][]string
	inElection    bool
}

const INACTIVE_LEADER = -1

const PROCESS_LIST_FILE = "name_ip.csv"

func getProcessList(id int) [][]string {
	processList := [][]string{}

	file, err := os.Open(PROCESS_LIST_FILE)
	if err != nil {
		fmt.Printf("Error opening process list file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error reading process list file: %v\n", err)
			os.Exit(1)
		}
		if record[0] == fmt.Sprintf("reviver-%d", id) {
			continue
		}
		processList = append(processList, record)
	}
	return processList
}

func NewNode(id int, stopContext context.Context) *Node {
	peers := []*Peer{}

	serverAddr, err := net.ResolveTCPAddr("tcp", ":8000")
	if err != nil {
		fmt.Printf("Error resolving server address: %v\n", err)
		os.Exit(1)
	}

	processList := getProcessList(id)

	return &Node{
		name:          fmt.Sprintf("reviver-%d", id),
		id:            id,
		peers:         peers,
		serverAddr:    serverAddr,
		stateLock:     sync.Mutex{},
		state:         NodeStateFollower,
		currentLeader: INACTIVE_LEADER,
		stopContext:   stopContext,
		wg:            sync.WaitGroup{},
		electionLock:  &sync.Mutex{},
		stopLeader:    make(chan struct{}),
		processList:   processList,
	}
}

func (n *Node) Close() {
	if n.serverConn != nil {
		if err := n.serverConn.Close(); err != nil {
			fmt.Printf("Error closing server connection: %v\n", err)
		}
	}
	for _, peer := range n.peers {
		peer.Close()
	}
}

func (n *Node) Run() {
	fmt.Printf("Running reviver %d\n", n.id)
	go shared.RunUDPListener(8080)
	n.wg.Add(1)
	go n.Listen() // solo responde por esta conexión TCP, no arranca la topología nunca (no PING, no ELECTION, etc)
	time.Sleep(2 * time.Second)
	go n.StartElection() // siempre queremos arrancar una eleccion cuando se levanta el nodo
	n.wg.Done()

	<-n.stopContext.Done()
}

func (n *Node) StartElection() {
	n.electionLock.Lock()
	defer n.electionLock.Unlock()
	n.ChangeState(NodeStateCandidate)
	n.inElection = true
	defer func() { n.inElection = false }()

	// Keep track of which peers we're waiting for responses from
	waitingResponses := make(map[int]bool)
	responseChan := make(chan int)
	timeoutChan := time.After(ElectionTimeout)

	// Send election messages only to higher-numbered peers
	for _, peer := range n.peers {
		if peer.id > n.id {
			// Send election message
			if err := peer.Send(Message{
				PeerId: n.id,
				Type:   MessageTypeElection,
			}); err != nil {
				continue
			}

			waitingResponses[peer.id] = true

			// Start a goroutine to wait for OK response from this peer
			go func(peerId int) {
				response := new(Message)
				if err := peer.conn.SetReadDeadline(time.Now().Add(OkResponseTimeout)); err != nil {
					log.Printf("Error setting read deadline for peer %d: %v", peerId, err)
					return
				}
				if err := peer.decoder.Decode(response); err != nil {
					return
				}

				if response.Type == MessageTypeOk {
					responseChan <- peerId
				}
			}(peer.id)
		}
	}

	// If no higher-numbered peers, become leader immediately
	if len(waitingResponses) == 0 {
		go n.BecomeLeader()
		return
	}

	// Wait for responses or timeout and act accordingly
electionLoop:
	for len(waitingResponses) > 0 {
		select {
		case peerId := <-responseChan:
			delete(waitingResponses, peerId)
			// log.Printf("Received OK from peer %d", peerId)
			n.stateLock.Lock()
			if n.state == NodeStateCandidate {
				n.state = NodeStateWaitingCoordinator
				n.stateLock.Unlock()
				break electionLoop
			}
			n.stateLock.Unlock()

		case <-timeoutChan:
			// If we timeout waiting for responses, we become the leader if we are a candidate
			// or we wait for coordinator if we are waiting for one
			if n.GetState() == NodeStateCandidate {
				go n.BecomeLeader()
			} else {
				go n.waitForCoordinator()
			}
			return
		}
	}

	go n.waitForCoordinator()
}

func (n *Node) waitForCoordinator() {
	<-time.After(ElectionTimeout + OkResponseTimeout)
	if n.GetState() == NodeStateWaitingCoordinator {
		go n.StartElection()
	}
}

func (n *Node) startFollowerLoop(leaderId int) {
	log.Printf("Node %d following leader %d\n", n.id, leaderId)
	for {
		select {
		case <-n.stopContext.Done():
			return
		case <-time.After(PingToLeaderTimeout):
			n.stateLock.Lock()
			if n.currentLeader != leaderId {
				n.stateLock.Unlock()
				return
			}
			n.stateLock.Unlock()
			var leaderPeer *Peer
			for _, peer := range n.peers {
				if peer.id == leaderId {
					leaderPeer = peer
					break
				}
			}

			if err := leaderPeer.Send(Message{PeerId: n.id, Type: MessageTypePing}); err != nil {
				go n.StartElection()
				continue
			}
			if err := leaderPeer.conn.SetReadDeadline(time.Now().Add(PongTimeout)); err != nil {
				log.Printf("Error setting read deadline for leader %d: %v", leaderId, err)
				continue
			}
			response := new(Message)
			err := leaderPeer.decoder.Decode(response)
			if errors.Is(err, os.ErrDeadlineExceeded) || err == io.EOF {
				if n.GetCurrentLeader() != leaderId { // si es distinto, significa que el lider cambió y ya no hay que tirarle ping a ese
					continue
				}
				n.ChangeState(NodeStateFollower)
				n.SetCurrentLeader(INACTIVE_LEADER)
				go n.StartElection()
				return
			}
			if err != nil && !errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("Error decoding response from leader %d: %v", leaderId, err)
				continue
			}
			if response.Type != MessageTypePong {
				log.Printf("Node %d received wrong message type %d from leader %d\n", n.id, response.Type, leaderId)
				continue
			}
		}
	}
}

func (n *Node) BecomeLeader() {
	oldLeader := n.GetCurrentLeader()
	n.ChangeState(NodeStateCoordinator)
	n.SetCurrentLeader(n.id)

	// Send coordinator message to all peers
	for _, peer := range n.peers {
		if err := peer.Send(Message{PeerId: n.id, Type: MessageTypeCoordinator}); err != nil {
			log.Printf("Error sending coordinator message to peer %d with ip %s: %v", peer.id, peer.ip.String(), err)
		}
	}

	if oldLeader != n.id {
		go n.StartLeaderLoop()
	}
}

func (n *Node) StartLeaderLoop() {
	log.Printf("Node %d is the new leader\n", n.id)

	stopResurrecters, cancel := context.WithCancel(context.Background())
	resurrecter := NewResurrecter(n.processList, stopResurrecters)
	go resurrecter.Start()
	for {
		select {
		case <-n.stopContext.Done():
			cancel()
			return
		case <-n.stopLeader:
			resurrecter.Close()
			cancel()
			return
		}
	}
}

func (n *Node) GetCurrentLeader() int {
	return n.currentLeader
}

func (n *Node) SetCurrentLeader(leaderId int) {
	n.currentLeader = leaderId
}

func (n *Node) GetState() NodeState {
	return n.state
}

func (n *Node) ChangeState(state NodeState) {
	n.state = state
}

func (n *Node) CreateTopology(reviverNodes int) {
	for i := 1; i <= reviverNodes; i++ {
		if i == n.id {
			continue
		}
		peerName := fmt.Sprintf("reviver-%d", i)
		peer := NewPeer(i, &peerName)
		if peer != nil {
			n.peers = append(n.peers, peer)
		}
	}
}

// Listen will be executed by every node, it will keep an eye on incoming nodes that
// want to start a connection with this node. This communication will act as a client
// server communication, making this node the server and the incoming node the client.
// This allows us to break the full duplex communication, resulting in a more simple
// implementation.
func (n *Node) Listen() {
	serverConn, err := net.ListenTCP("tcp", n.serverAddr)
	if err != nil {
		fmt.Printf("Error listening on server address %s: %v\n", n.serverAddr.String(), err)
		os.Exit(1)
	}
	n.serverConn = serverConn

	for {
		select {
		case <-n.stopContext.Done():
			return
		default:
			conn, err := serverConn.AcceptTCP()
			if err != nil && errors.Is(err, io.EOF) {
				return
			}
			if err != nil {
				// fmt.Printf("Error accepting connection: %v\n", err)
				continue
			}
			go n.RespondToPeer(conn)
		}
	}
}

// RespondToPeer will handle the communication with the incoming node, decoding the messages
// and responding them with the appropriate ones. This messages could be pings,
// elections or coordinations.
func (n *Node) RespondToPeer(conn *net.TCPConn) {
	n.wg.Wait()

	read := gob.NewDecoder(conn)
	encoder := gob.NewEncoder(conn)

	for {
		message := new(Message)
		err := read.Decode(message)

		if err != nil && err != io.EOF {
			if err := conn.Close(); err != nil {
				fmt.Printf("Error closing connection: %v\n", err)
			}
			break
		}

		if err == io.EOF {
			if err := conn.Close(); err != nil {
				fmt.Printf("Error closing connection: %v\n", err)
			}
			break
		}

		n.handleMessage(message, encoder)
	}
	n.ClosePeer(strings.Split(conn.RemoteAddr().String(), ":")[0])
}

func (n *Node) ClosePeer(ip string) {
	for _, peer := range n.peers {
		if strings.Split(peer.ip.String(), ":")[0] == ip {
			peer.Close()
			break
		}
	}
}

// handleMessage implements the logic to respond to the different types of messages
// that the node receives from a peer. This set of messages only includes those that
// matter when acting as the server for incoming nodes (i.e. the messages a node sends
// to its peers, not the ones that come from the outside).
func (n *Node) handleMessage(message *Message, encoder *gob.Encoder) {
	switch message.Type {
	case MessageTypePing:
		n.handlePing(encoder)
	case MessageTypeElection:
		n.handleElection(encoder)
	case MessageTypeCoordinator:
		n.handleCoordinator(message)
	}
}

func (n *Node) handleCoordinator(message *Message) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()

	log.Printf("Node %d received coordinator message from node %d", n.id, message.PeerId)
	if n.id > message.PeerId {
		go n.StartElection()
		return
	}

	if n.state == NodeStateCoordinator {
		log.Printf("Stopping leader loop")
		n.stopLeader <- struct{}{}
	}

	n.state = NodeStateFollower
	if n.currentLeader == message.PeerId {
		return
	}
	n.currentLeader = message.PeerId
	go n.startFollowerLoop(message.PeerId)
}

func (n *Node) handlePing(encoder *gob.Encoder) {
	msg := Message{PeerId: n.id, Type: MessageTypePong}
	if err := encoder.Encode(msg); err != nil {
		fmt.Printf("Error sending pong message: %v\n", err)
	}
}

func (n *Node) handleElection(encoder *gob.Encoder) {
	msg := Message{PeerId: n.id, Type: MessageTypeOk}
	if err := encoder.Encode(msg); err != nil {
		fmt.Printf("Error sending ok message: %v\n", err)
	}
	if !n.inElection {
		go n.StartElection()
	}
}