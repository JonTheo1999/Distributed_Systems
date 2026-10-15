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
    userCh := make(chan *pb.Broadcast, 50) // klientens personlige kanal. Kan indeholde op til 50 beskeder. kan ændres

	s.lock.Lock() // Låser således at andre goroutines ikke kan pille ved serverens felter før vi er færdige
	s.userChannels[req.GetUsername()] = userCh // putter brugeren og brugerens kanal i mappet (hashmap)
	s.clock = max(s.clock, req.Timestamp) + 1 // modtag-reglen in action. serveren modtager timestamp fra klienten og sikre at højeste værdi altid er serverens ur.
	s.clock++ // tæller serverens ur op

	msg := &pb.Broadcast{ // Bygger den besked, der skal broadcastes til alle brugere som er inde i chitchat
		Username: "Server",
		Timestamp: s.clock,
		MsgContent: fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", req.GetUsername(), s.clock),
	}

	log.Printf("[Server] [Join] %s joined at logical time %d", req.GetUsername(), s.clock) // Logger join hændelsen

	for name, ch :=  range s.userChannels { // for-loop der sender broadcast-besked ud til alle brugers kanaler
		select {
		case ch <- msg:
			log.Printf("[Server] [Broadcast]  %s received broadcast", name)
		default:
			log.Printf("[Server] [Broadcast]  %s did not receive broadcast. Channel is full", name)
		}
	}
	s.lock.Unlock() // Låser op igen så andre goroutines kan bruge serverens felter

	for { // Dette er en uendlig løkke som bliver ved så længe at brugeren har forbindelse til chitchat
		select {
		case msg, ok := <- userCh: // Modtager ny besked i brugerens kanal
			if !ok { // kanalen er lukket og brugeren har med vilje forladt chitchat
				return nil
			} else { // Almindelig besked sendt til brugeren
				err := stream.Send(msg)
		
				if err != nil { // Hvis brugeren forbindelse fejler, logger vi det og fjerne brugeren fra listen af brugere
					log.Printf("[Server] [Disconnect] %s disconnected: %v", req.GetUsername(), err)

					s.lock.Lock()
					delete(s.userChannels, req.GetUsername())
					s.lock.Unlock()
					return err
				}

				log.Printf("[Server] [Delivery] delivered to %s: from=%s time=%d msg=%q", // logger hvis beskeden kom igennem til brugeren
    			req.GetUsername(), msg.Username, msg.Timestamp, msg.MsgContent)
			}
		case <- stream.Context().Done(): // forbindelsen døde. Her fanger vi disconnecten og rydder op
			s.lock.Lock()
			delete(s.userChannels, req.GetUsername())
			log.Printf("[Server] [Disconnect] %s left the chat at logical time %d", req.GetUsername(), s.clock)
			s.lock.Unlock()
			
			return stream.Context().Err()
		}
	}	
}

func (s * chitChatServer) Publish(ctx context.Context, pubReq *pb.PublishRequest) (pubRep *pb.PublishReply, e error) {
	if utf8.RuneCountInString(pubReq.MsgContent) > 128 || pubReq.MsgContent == "" { // tjekker om en besked er for lang eller tom
		log.Printf("[Server] [Publish] Message sent by %s is too long or empty", pubReq.GetUsername())
		return nil, fmt.Errorf("Message is invalid")
	}
	
	s.lock.Lock() // Låser således at andre goroutines ikke kan pille ved serverens felter før vi er færdige
	s.clock = max(s.clock, pubReq.Timestamp) + 1 // modtag-reglen in action. serveren modtager timestamp fra klienten og sikre at højeste værdi altid er serverens ur.
	s.clock++ // tæller serverens ur op (send-reglen)

	msg := &pb.Broadcast{ // Bygger den besked som en bruger gerne vil sende til alle brugere som er inde i chitchat
		Username: pubReq.Username,
		Timestamp: s.clock,
		MsgContent: pubReq.MsgContent,
	}

	serTs := s.clock // gemmer serverens tid i lokal variabel

	for name, ch :=  range s.userChannels { // for-loop der sender besked ud til alle brugers kanaler
		select {
		case ch <- msg:
			log.Printf("[Server] [Broadcast]  %s received broadcast", name)
		default:
			log.Printf("[Server] [Broadcast]  %s did not receive broadcast. Channel is full", name)
		}
	}
	s.lock.Unlock() // Låser op igen så andre goroutines kan bruge serverens felter

	log.Printf("[Server] [Publish] %s sent message: %s; at logical time %d", pubReq.GetUsername(), pubReq.MsgContent, serTs) // Logger at beskeden blev sendt

	reply := &pb.PublishReply{ // svarer afsenderen at beskeden blev sendt. sender timestamp så klient kan opdatere sit ur
		Timestamp: serTs,
	}
	
	return reply, nil // sender reply til klienten
}

func (s * chitChatServer) Leave(ctx context.Context, leaveReq *pb.LeaveRequest) (leaveRep *pb.LeaveReply, e error) {
	s.lock.Lock() // Låser således at andre goroutines ikke kan pille ved serverens felter før vi er færdige
	ch, ok := s.userChannels[leaveReq.Username] // Slår brugeren, som anmoder om at forlade chitchat, op

		if !ok { // Hvis brugeren ikke findes i listen over brugere, returnere vi med en fejlbesked
			log.Printf("[Server] [Leave] rejected: %s is not in the chat", leaveReq.GetUsername())
			s.lock.Unlock()
			return nil, fmt.Errorf("User does not exist")
		} 

	delete(s.userChannels, leaveReq.Username) // Vi sletter brugeren fra chitchats liste over medlemmer
	close(ch) // vi lukker brugerens kanal

	s.clock = max(s.clock, leaveReq.Timestamp) + 1 // modtag-reglen in action. serveren modtager timestamp fra klienten og sikre at højeste værdi altid er serverens ur.
	s.clock++ // tæller serverens ur op (send-reglen)
	serTs := s.clock // gemmer serverens tid i lokal variabel

	msg := &pb.Broadcast{ // Bygger den besked, der skal broadcastes til alle brugere som er inde i chitchat
		Username: "Server",
		Timestamp: serTs,
		MsgContent: fmt.Sprintf("Participant %s left Chit Chat at logical time %d", leaveReq.GetUsername(), serTs),
	}

	for name, ch :=  range s.userChannels { // for-loop der sender besked ud til alle brugers kanaler
		select {
		case ch <- msg:
			log.Printf("[Server] [Broadcast]  %s received broadcast", name)
		default:
			log.Printf("[Server] [Broadcast]  %s did not receive broadcast. Channel is full", name)
		}
	}
	s.lock.Unlock() // Låser op igen så andre goroutines kan bruge serverens felter

	log.Printf("[Server] [Leave] %s left Chit Chat at logical time %d", leaveReq.GetUsername(), serTs) // logger beskeden om at en bruger forlod chatten

	reply := &pb.LeaveReply{ // svarer afsenderen at vedkommende har forladt chitchat. sender timestamp så klient kan opdatere sit ur
		Timestamp: serTs,
	}

	return reply, nil // sender reply til klienten
}
