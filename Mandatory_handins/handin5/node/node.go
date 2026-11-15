package main

import (
	"context"
	//"net"
	//"log"

	pb "handin5/auction"
	//"google.golang.org/grpc"
)

type node struct {
	pb.UnimplementedAuctionServer
    pb.UnimplementedReplicationServer

}

func main() {

}

func (n *node) Bid(ctx context.Context, req *pb.BidRequest) (rep *pb.BidReply, e error) {
	return nil, nil
}

func (n *node) Result(ctx context.Context, req *pb.ResultRequest) (rep *pb.ResultReply, e error) {
	return nil, nil
}

func (n *node) Update(ctx context.Context, req *pb.UpdateRequest) (rep *pb.UpdateReply, e error) {
	return nil, nil
}

func (n *node) Alive(ctx context.Context, req *pb.AliveRequest) (rep *pb.AliveReply, e error) {
	return nil, nil
}