package main

import (
	"context"
	pb "gm/grpc"
	"google.golang.org/grpc"
	"sync"
	"net"
	"log"
	"fmt"
	"unicode/utf8"
)

type chitChatServer struct {
    pb.UnimplementedChitChatServer   // påkrævet af gRPC, se nedenfor

	userChannels map[string]chan *pb.Broadcast
	lock sync.Mutex
	clock int64
}

func main() {
	 chitChatSer := &chitChatServer{userChannels: make(map[string]chan *pb.Broadcast)}
	 host := "localhost:5000"

	 listener, err := net.Listen("tcp", host)

	if err != nil {
	log.Fatalf("[Server] failed to listen on port %s: %v", host, err)
	}

	gRPCser := grpc.NewServer()

	pb.RegisterChitChatServer(gRPCser, chitChatSer)

	log.Println("[Server] is listening on: ", host)

	if err := gRPCser.Serve(listener); err != nil {
		log.Fatalf("[Server] stopped working: %v", err)
	}
}

func (s *chitChatServer) Join(req *pb.JoinRequest, stream grpc.ServerStreamingServer[pb.Broadcast]) (e error) {
    userCh := make(chan *pb.Broadcast, 50)

	s.lock.Lock()
	s.userChannels[req.GetUsername()] = userCh
	s.clock = max(s.clock, req.Timestamp) + 1
	s.clock++

	msg := &pb.Broadcast{
		Username: "Server",
		Timestamp: s.clock,
		MsgContent: fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", req.GetUsername(), s.clock),
	}

	for name, ch :=  range s.userChannels {
		select {
		case ch <- msg:
			log.Printf("[Server] [Broadcast]  %s received broadcast", name)
		default:
			log.Printf("[Server] [Broadcast]  %s did not receive broadcast. Channel is full", name)
		}
	}
	s.lock.Unlock()

	for {
		select {
		case msg, ok := <- userCh:
			if !ok {
				return nil
			} else {
				err := stream.Send(msg)
		
				if err != nil {
					log.Printf("[Server] %s disconnected: %v", req.GetUsername(), err)

					s.lock.Lock()
					delete(s.userChannels, req.GetUsername())
					s.lock.Unlock()
					return err
				}

				log.Printf("[Server] [Delivery] delivered to %s: from=%s time=%d msg=%q",
    			req.GetUsername(), msg.Username, msg.Timestamp, msg.MsgContent)
			}
		case <- stream.Context().Done():
			s.lock.Lock()
			delete(s.userChannels, req.GetUsername())
			log.Printf("[Server] %s left the chat at logical time %d", req.GetUsername(), s.clock)
			s.lock.Unlock()
			
			return stream.Context().Err()
		}
	}	
}

func (s * chitChatServer) Publish(ctx context.Context, pubReq *pb.PublishRequest) (pubRep *pb.PublishReply, e error) {
	if utf8.RuneCountInString(pubReq.MsgContent) > 128 || pubReq.MsgContent == "" {
		log.Printf("[Server] [Publish] Message is too long or empty")
		return nil, fmt.Errorf("Message is invalid")
	}
	
	s.lock.Lock()
	s.clock = max(s.clock, pubReq.Timestamp) + 1
	s.clock++

	msg := &pb.Broadcast{
		Username: pubReq.Username,
		Timestamp: s.clock,
		MsgContent: pubReq.MsgContent,
	}

	serTs := s.clock

	for name, ch :=  range s.userChannels {
		select {
		case ch <- msg:
			log.Printf("[Server] [Broadcast]  %s received broadcast", name)
		default:
			log.Printf("[Server] [Broadcast]  %s did not receive broadcast. Channel is full", name)
		}
	}
	s.lock.Unlock()

	log.Printf("[Server] [Publish] %s sent message: %s; at logical time %d", pubReq.Username, pubReq.MsgContent, serTs)

	reply := &pb.PublishReply{
		Timestamp: serTs,
	}
	
	return reply, nil
}

func (s * chitChatServer) Leave(ctx context.Context, leaveReq *pb.LeaveRequest) (leaveRep *pb.LeaveReply, e error) {
	return nil, nil
}
