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
	"os"
	"os/signal"
)

type chitChatServer struct {
    pb.UnimplementedChitChatServer   // påkrævet af gRPC

	userChannels map[string]chan *pb.Broadcast
	lock sync.Mutex
	clock int64
}

func main() {
	 chitChatSer := &chitChatServer{userChannels: make(map[string]chan *pb.Broadcast)}  // Server instansen. Har et map over brugere som peger på hver deres kanal
	 host := "localhost:5000" // Statisk host. kan ændres senere

	 listener, err := net.Listen("tcp", host) // Dette er "døren" som nye forbindelser banker på

	if err != nil { // Sikkerhed hvis "døren" ikke kan åbnes
		log.Fatalf("[Server] failed to listen on port %s: %v", host, err)
	}

	gRPCser := grpc.NewServer() // Laver gRPC-serveren

	pb.RegisterChitChatServer(gRPCser, chitChatSer) // forbinder vores chat på vores gRPC-server

	log.Printf("[Server] [Startup] Server is listening on %s", host) // logger opstart

	sigCh := make(chan os.Signal, 1) // Fanger nedlukning af serveren således at fx Ctrl+C "lukker pænt" 
  	signal.Notify(sigCh, os.Interrupt) // og indsætter det i sigCh kanalen 
  	
	go func() { // lille anonym funktion. Denne funktion kører i baggrunden og venter bare på at noget kommer ind på sigCh kanalen
      <-sigCh
    	log.Printf("[Server] [Shutdown] Server is shutting down") // logger nedlukning
		gRPCser.Stop() // lukker ALLE forbindelser
  	}()

	err = gRPCser.Serve(listener) // serveren tager imod forbindelser. Lægges i lokal variabel for at fange fejl. SE NEDEUNDER

	if err != nil { // hvis serveren fejler, fanger vi og udprinter pænt
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

	log.Printf("[Server] [Join] %s joined at logical time %d", req.GetUsername(), s.clock)

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
					log.Printf("[Server] [Disconnect] %s disconnected: %v", req.GetUsername(), err)

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
			log.Printf("[Server] [Disconnect] %s left the chat at logical time %d", req.GetUsername(), s.clock)
			s.lock.Unlock()
			
			return stream.Context().Err()
		}
	}	
}

func (s * chitChatServer) Publish(ctx context.Context, pubReq *pb.PublishRequest) (pubRep *pb.PublishReply, e error) {
	if utf8.RuneCountInString(pubReq.MsgContent) > 128 || pubReq.MsgContent == "" {
		log.Printf("[Server] [Publish] Message sent by %s is too long or empty", pubReq.GetUsername())
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

	log.Printf("[Server] [Publish] %s sent message: %s; at logical time %d", pubReq.GetUsername(), pubReq.MsgContent, serTs)

	reply := &pb.PublishReply{
		Timestamp: serTs,
	}
	
	return reply, nil
}

func (s * chitChatServer) Leave(ctx context.Context, leaveReq *pb.LeaveRequest) (leaveRep *pb.LeaveReply, e error) {
	s.lock.Lock()
	ch, ok := s.userChannels[leaveReq.Username]

		if !ok {
			log.Printf("[Server] [Leave] rejected: %s is not in the chat", leaveReq.GetUsername())
			s.lock.Unlock()
			return nil, fmt.Errorf("User does not exist")
		} 

	delete(s.userChannels, leaveReq.Username)
	close(ch)

	s.clock = max(s.clock, leaveReq.Timestamp) + 1
	s.clock++
	serTs := s.clock

	msg := &pb.Broadcast{
		Username: "Server",
		Timestamp: serTs,
		MsgContent: fmt.Sprintf("Participant %s left Chit Chat at logical time %d", leaveReq.GetUsername(), serTs),
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

	log.Printf("[Server] [Leave] %s left Chit Chat at logical time %d", leaveReq.GetUsername(), serTs)

	reply := &pb.LeaveReply{
		Timestamp: serTs,
	}

	return reply, nil
}
