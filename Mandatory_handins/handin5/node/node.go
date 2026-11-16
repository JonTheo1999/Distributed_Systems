package main

import (
	"context"
	"sync"
	"time"

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

	leader bool // Så noden ved om den er leader
	highestBid int64 // højeste bud
	highestBidder int64 // personen der gav højeste bud. Har sat typen til at være det samme som client.go's id felt

	// Et sluttidspunkt til at afslutte auktionen. 
	// Jeg har lagt op til at starte timeren når første bid bliver anerkendt. 
	// Det gør timmeren nem at implementere, men gør at auktionen aldrig stopper, 
	// hvis ikke der kommer nogle bud. For denne opgave tror jeg det er fint.
	// hvad tænker I?
	endAuction time.Time 

	lock sync.Mutex // en lås således at highestBid kun ændres af en goroutine ad gangen

	// Hvis en klient sender samme bud flere gange. 
	// Bruges egentligt kun hvis leader crasher før leader har givet svar. 
	retryBid map[int64]latestBid 
}

// bruges kun i retryBid-feltet. indeholder en acknowledgement og en request id.
type latestBid struct {
	ack pb.Ack
	reqId int64
}

func main() {

}

// AuctionServer metode
func (n *node) Bid(ctx context.Context, req *pb.BidRequest) (rep *pb.BidReply, e error) {
	return nil, nil
}

// AuctionServer metode
func (n *node) Result(ctx context.Context, req *pb.ResultRequest) (rep *pb.ResultReply, e error) {
	return nil, nil
}

// ReplicationServer metode
func (n *node) Update(ctx context.Context, req *pb.UpdateRequest) (rep *pb.UpdateReply, e error) {
	return nil, nil
}

// ReplicationServer metode
func (n *node) Alive(ctx context.Context, req *pb.AliveRequest) (rep *pb.AliveReply, e error) {
	return nil, nil
}