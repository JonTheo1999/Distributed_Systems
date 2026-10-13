package main

import (
	"context"
	pb "gm/grpc"
	"google.golang.org/grpc"
	"sync"
	"net"
)

type chitChatServer struct {
    pb.UnimplementedChitChatServer   // påkrævet af gRPC, se nedenfor

	userChannels map[string]chan *pb.Broadcast
	lock sync.Mutex
	clock int64
}

func main() {
	 chitChatSer := &chitChatServer{userChannels: make(map[string]chan *pb.Broadcast)}

	 listener, err := net.Listen("tcp", "localhost:5000")

	if err != nil {
	log.Fatalf("[Server] failed to listen on port 5000: %v", err)
	}

	gRPCser := grpc.NewServer()
}

func (s *chitChatServer) Join(req *pb.JoinRequest, stream grpc.ServerStreamingServer[pb.Broadcast]) (e error) {
    return nil
}

func (s * chitChatServer) Publish(ctx context.Context, pubReq *pb.PublishRequest) (pubRep *pb.PublishReply, e error) {
	return nil, nil
}

func (s * chitChatServer) Leave(ctx context.Context, leaveReq *pb.LeaveRequest) (leaveRep *pb.LeaveReply, e error) {
	return nil, nil
}
