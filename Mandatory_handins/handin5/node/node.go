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
	pb.UnimplementedAuctionServer
    pb.UnimplementedReplicationServer
	
	otherNode pb.ReplicationClient

	leader bool
	highestBid int64
	highestBidder int64
	endAuction time.Time
	lock sync.Mutex
	retryBid map[int64]latestBid
}

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