package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	proto "gm/grpc"
)

type Node struct {
	proto.UnimplementedRicartAgrawalaServer

	id   int64
	port string

	lock      sync.Mutex
	cond      *sync.Cond
	state     state
	clock     int64
	requestTS int64

	peers map[int64]proto.RicartAgrawalaClient
}

type state int

const (
	idle    state = iota //released
	waiting              //wanted
	holding              //held
)

var nextNodeID int64
var nextNodeIDMutex sync.Mutex

var addrs = map[int64]string{
	0: "localhost:5000",
	1: "localhost:5001",
	2: "localhost:5002",
}

func main() {
	id := flag.Int("id", 0, "id of this node (0, 1, or 2)")
	flag.Parse()

	addr, ok := addrs[*id]
	if !ok {
		log.Fatalf("[Node] [Error] Invalid node id: %d", *id)
	}

	n := newNode(*id, addr)

	lis, err := net.Listen("tcp", n.port)
	if err != nil {
		log.Fatalf("[Node] [Error] Failed to listen on port %s: %v", n.port, err)
	}

	gRPCser := grpc.NewServer()
	proto.RegisterRicartAgrawalaServer(gRPCser, n)

	go func() {
		if err := gRPCser.Serve(lis); err != nil {
			log.Fatalf("[Node] [Error] Failed to serve gRPC server: %v", err)
		}
	}()

	n.connectToPeers(addrs)

	go func() {
		for {
			time.Sleep(time.Duration(1+rand.Intn(3)) * time.Second) // vent mellem 1 og 3 sekunder
			n.enter()
			log.Printf("[Node %d] [Critical Section] Entering critical section", n.id)
			time.Sleep(time.Second) // simulerer at være i critical section i 1 sekund
			n.exit()
		}
	}()

	sigCh := make(chan os.Signal, 1)   // Fanger nedlukning af noden således at fx Ctrl+C "lukker pænt"
	signal.Notify(sigCh, os.Interrupt) // og indsætter det i sigCh kanalen
	<-sigCh

	log.Printf("[Node %d] [Shutdown] Shutting down node", n.id)
	gRPCser.stop()
}

func newNode(id int64, port string) *Node {
	n := &Node{
		id:    id,
		port:  port,
		peers: make(map[int64]proto.RicartAgrawalaClient),
	}
	n.cond = sync.NewCond(&n.lock)
	return n
}

func (n *Node) connectToPeers(addrs map[int64]string) {
	for id, addr := range addrs {
		if id == n.id {
			continue // skip self
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("[Node] [Error] Failed to connect to peer %s: %v", n.id, id, err)
		}
		n.peers[id] = proto.NewRicartAgrawalaClient(conn)
		log.Printf("[Node] [Success] Connected to peer %d at %s", n.id, id, addr)
	}
}

func (n *Node) enter() {
	n.lock.Lock()     // låser for at sikre at clock og timestamp ikke ændres imens vi sender
	n.state = waiting // ændrer state til
	n.clock++
	n.requestTS = n.clock
	req := &proto.RequestAccess{Id: n.id, Timestamp: n.requestTS} // sender request til peers
	n.lock.Unlock()

	log.Printf("[Node %d] [Request] Sending request to peers (ts=%d)", n.id, req.Timestamp)

	var wg sync.WaitGroup
	for id, peer := range n.peers {
		wg.Add(1)
		go func(id int64, peer proto.RicartAgrawalaClient) {
			defer wg.Done()

			reply, err := peer.Request(context.Background(), req, grpc.WaitForReady(true))
			if err != nil {
				log.Fatalf("[Node %d] [Error] Failed to send request to peer %d: %v", n.id, id)
			}
			n.lock.Lock()
			n.clock = max(n.clock, reply.Timestamp) + 1
			n.lock.Unlock()
			log.Printf("[Node %d] [Reply] Received consent from node %d (ts=%d)", n.id, id)
		}(id, peer)
	}

	wg.Wait() // venter på at alle peers har svaret

	n.lock.Lock()
	n.state = holding
	n.lock.Unlock()

}

func (n *Node) Request(ctx context.Context, req *proto.RequestAccess) (*proto.RequestConsent, error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.clock

	// Opdaterer clocken baseret på den modtagne timestamp
	if req.Timestamp > n.clock {
		n.clock = req.Timestamp + 1
	}
	n.clock++

	log.Printf("[Node %d] [Request] Received request from node %d (ts=%d)", n.id, req.Id, req.Timestamp)

	// Hvis noden er i holding state, eller hvis den er i waiting state og har højere prioritet end den anmodende node, skal den vente med at give samtykke
	for n.state == holding || (n.state == waiting && n.hasPriority(req)) {
		n.cond.Wait() // venter på at state ændres til idle
	}

	log.Printf("[Node %d] [Reply] Granting node %d", n.id, req.Id)
	return &proto.RequestConsent{Id: n.id, Timestamp: n.clock}, nil
}

func (n *Node) hasPriority(req *proto.RequestAccess) bool {
	if n.requestTS != req.Timestamp {
		return n.requestTS < req.Timestamp // lavere timestamp har højere prioritet
	}
	return n.id < req.Id // lavere id har højere prioritet
}

func (n *Node) exit() {
	n.state = idle                  // ændrer state til idle efter at have forladt critical section
	for _, peers := range n.peers { // itererer over peers og sender release til hver enkelt

	}
}
