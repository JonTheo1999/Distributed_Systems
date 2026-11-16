package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"

	"google.golang.org/grpc"
	//"net"
	//"log"

	pb "handin5/auction"
	//"google.golang.org/grpc"
)

// En node skal kunne kommunikere to veje. én til klienterne (kunderne i auktionen)
// og til andre noder (replikere data og bekræfte "i live" - tilstand). Derfor
// implementerer noden både AuctionServer og ReplicationServer.
type node struct {

	// Noden "arver" metoderne fra disse structs.
	// gRPC kræver det, fordi server-interfaces har en privat mustEmbed-metode,
	// som kun kan fås på denne måde. De arvede metoder svarer bare "not implemented"
	// og bliver erstattet af vores egne Bid, Result, Update og Alive.
	pb.UnimplementedAuctionServer
	pb.UnimplementedReplicationServer

	// Denne er unembedded i modsætning til de to andre.
	// Det er fordi en node fx. ikke skal kalde Alive() på sig
	// selv men på dens peer. havde den været embedded ville den kalde sig selv
	otherNode pb.ReplicationClient

	leader        bool  // Så noden ved om den er leader
	highestBid    int64 // højeste bud
	highestBidder int64 // personen der gav højeste bud. Har sat typen til at være det samme som client.go's id felt

	// Et sluttidspunkt til at afslutte auktionen.
	// Jeg har lagt op til at starte nedtællingen når første bid bliver anerkendt.
	// Det gør timeren nem at implementere, men gør at auktionen aldrig stopper,
	// hvis ikke der kommer nogle bud. For denne opgave tror jeg det er fint.
	// hvad tænker I?
	endAuction time.Time

	lock sync.Mutex // en lås således at fx. highestBid kun ændres af én goroutine ad gangen

	// Hvis en klient sender samme bud flere gange.
	// Bruges egentligt kun hvis leader crasher før leader har givet svar.
	retryBid map[int64]latestBid
}

// bruges kun i retryBid-feltet. indeholder en acknowledgement og en request id.
type latestBid struct {
	ack   pb.Ack
	reqId int64
}

var addrs = map[int64]string{
	0: "localhost:5000",
	1: "localhost:5001",
}

func main() {
	id := flag.Int64("id", 0, "id of this node (0 = leader, 1 = backup)")
	flag.Parse()

	logFile, err := os.OpenFile(fmt.Sprintf("node%d.log", *id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	addr, ok := addrs[*id]
	if !ok {
		log.Fatalf("[Node] [Error] Invalid node id: %d", *id)
	}

	n := &node{
		leader:     *id == 0,
		retryBid:   map[int64]latestBid{},
		endAuction: time.Now().Add(10 * time.Second), // sætter en slutdato for auktionen til 10 sekunder fra nu. Kan ændres.
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("[Node] [Error] Failed to listen on port %s: %v", addr, err)
	}

	gRPCser := grpc.NewServer()
	pb.RegisterAuctionServer(gRPCser, n)
	pb.RegisterReplicationServer(gRPCser, n)

	go func() {
		if err := gRPCser.Serve(lis); err != nil {
			log.Fatalf("[Node] [Error] Failed to serve gRPC server: %v", err)
		}
	}()

	log.Printf("[Node %d] [Startup] Listening on %s, leader=%v", *id, addr, n.leader)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Printf("[Node %d] [Shutdown] Node is shutting down", *id)
	gRPCser.Stop()
}

// AuctionServer metode
func (n *node) Bid(ctx context.Context, req *pb.BidRequest) (rep *pb.BidReply, e error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if last, ok := n.retryBid[req.ClientId]; ok && last.reqId == req.ReqId {
		return &pb.BidReply{Ack: last.ack}, nil
	}

	ack := pb.Ack_ACK_FAIL

	if req.Amount <= 0 {
		ack = pb.Ack_ACK_EXC
	} else if time.Now().Before(n.endAuction) && req.Amount > n.highestBid {
		n.highestBid = req.Amount
		n.highestBidder = req.ClientId
		ack = pb.Ack_ACK_SUCCESS
	}

	n.retryBid[req.ClientId] = latestBid{ack: ack, reqId: req.ReqId}
	log.Printf("[Node] [Bid] client %d (req %d) bid %d -> %v | highest is now %d by %d", req.ClientId, req.ReqId, req.Amount, ack, n.highestBid, n.highestBidder)
	return &pb.BidReply{Ack: ack}, nil
}

// AuctionServer metode
func (n *node) Result(ctx context.Context, req *pb.ResultRequest) (rep *pb.ResultReply, e error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	return &pb.ResultReply{
		HighestBidder: n.highestBidder,
		Amount:        n.highestBid,
		Over:          time.Now().After(n.endAuction),
	}, nil
}

// ReplicationServer metode
func (n *node) Update(ctx context.Context, req *pb.UpdateRequest) (rep *pb.UpdateReply, e error) {
	return nil, nil
}

// ReplicationServer metode
func (n *node) Alive(ctx context.Context, req *pb.AliveRequest) (rep *pb.AliveReply, e error) {
	return &pb.AliveReply{}, nil
}
