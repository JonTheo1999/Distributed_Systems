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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pb "handin5/auction"
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
	peerAlive     bool  // false når backup er crashet. Så holder leader op med at replikere
	highestBid    int64 // højeste bud
	highestBidder int64 // personen der gav højeste bud. Har sat typen til at være det samme som client.go's id felt

	// Et sluttidspunkt til at afslutte auktionen.
	// Nedtællingen starter når noden starter, så auktionen altid stopper.
	// Leader sender sluttidspunktet med i hver Update, så backup bruger leaders deadline.
	endAuction time.Time

	lock sync.Mutex // en lås således at fx. highestBid kun ændres af én goroutine ad gangen

	// Hvis en klient sender samme bud flere gange.
	// Bruges hvis leader crasher før klienten har fået svar.
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

	// forbindelse til den anden node (der er kun to noder, så peer er 1 - id)
	peerConn, err := grpc.NewClient(addrs[1-*id], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Node] [Error] Cannot create connection to peer: %v", err)
	}
	defer peerConn.Close()

	n := &node{
		otherNode:  pb.NewReplicationClient(peerConn),
		leader:     *id == 0,
		peerAlive:  true,
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

	if !n.leader {
		go n.watchLeader(*id)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	<-sigCh

	log.Printf("[Node %d] [Shutdown] Node is shutting down", *id)
	gRPCser.Stop()
}

// Backup pinger leader med Alive. Hvis leader ikke svarer inden for tidsgrænsen,
// er den crashet, og backup bliver leader. Fejl før det første vellykkede ping
// ignoreres, så backup ikke overtager bare fordi leader ikke er startet endnu.
func (n *node) watchLeader(id int64) {
	seen := false
	for {
		time.Sleep(1 * time.Second)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := n.otherNode.Alive(ctx, &pb.AliveRequest{})
		cancel()

		if err == nil {
			seen = true
			continue
		}
		if !seen {
			continue
		}

		n.lock.Lock()
		n.leader = true
		n.peerAlive = false
		n.lock.Unlock()
		log.Printf("[Node %d] [Failover] Leader is unreachable. This node is now the leader", id)
		return
	}
}

// Kaldes med lock holdt. Sender et vellykket bud til backup.
// Hvis backup ikke svarer, antager vi den er crashet og kører videre alene.
func (n *node) replicate(req *pb.BidRequest, rep *pb.BidReply) {
	if !n.peerAlive {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := n.otherNode.Update(ctx, &pb.UpdateRequest{
		ReqCopy:     req,
		BidCopy:     rep,
		ResultCopy:  &pb.ResultReply{HighestBidder: n.highestBidder, Amount: n.highestBid},
		EndTimeUnix: n.endAuction.UnixNano(),
	})
	if err != nil {
		n.peerAlive = false
		log.Printf("[Node] [Replication] Backup is unreachable (%v). Continuing without backup", err)
	}
}

// AuctionServer metode
func (n *node) Bid(ctx context.Context, req *pb.BidRequest) (rep *pb.BidReply, e error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	// kun leader må behandle bud. Fejl (ikke ACK_EXC) får klienten til at prøve en anden node.
	if !n.leader {
		return nil, status.Error(codes.Unavailable, "node is not the leader")
	}

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
	rep = &pb.BidReply{Ack: ack}

	// Kun vellykkede bud ændrer auktionens tilstand, så kun dem replikeres.
	// Backup skal have buddet FØR klienten får svar.
	if ack == pb.Ack_ACK_SUCCESS {
		n.replicate(req, rep)
	}

	log.Printf("[Node] [Bid] client %d (req %d) bid %d -> %v | highest is now %d by %d", req.ClientId, req.ReqId, req.Amount, ack, n.highestBid, n.highestBidder)
	return rep, nil
}

// AuctionServer metode
func (n *node) Result(ctx context.Context, req *pb.ResultRequest) (rep *pb.ResultReply, e error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	if !n.leader {
		return nil, status.Error(codes.Unavailable, "node is not the leader")
	}

	return &pb.ResultReply{
		HighestBidder: n.highestBidder,
		Amount:        n.highestBid,
		Over:          time.Now().After(n.endAuction),
	}, nil
}

// ReplicationServer metode. Kaldes af leader på backup.
func (n *node) Update(ctx context.Context, req *pb.UpdateRequest) (rep *pb.UpdateReply, e error) {
	n.lock.Lock()
	defer n.lock.Unlock()

	n.highestBid = req.ResultCopy.Amount
	n.highestBidder = req.ResultCopy.HighestBidder
	n.endAuction = time.Unix(0, req.EndTimeUnix)
	n.retryBid[req.ReqCopy.ClientId] = latestBid{ack: req.BidCopy.Ack, reqId: req.ReqCopy.ReqId}

	log.Printf("[Node] [Replication] Copied bid %d by client %d from leader", n.highestBid, n.highestBidder)
	return &pb.UpdateReply{}, nil
}

// ReplicationServer metode. Tager bevidst ikke låsen, så leader kan svare mens den venter på backup.
func (n *node) Alive(ctx context.Context, req *pb.AliveRequest) (rep *pb.AliveReply, e error) {
	return &pb.AliveReply{}, nil
}