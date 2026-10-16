package main

import (
	"bufio"
	"context"
	"fmt"
	pb "gm/grpc"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
	

type client struct {
    username string
	grpcCli pb.ChitChatClient
	lock sync.Mutex
	clock int64
	done chan struct{}
	leaveOnce sync.Once
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run ./client <username>")
		os.Exit(1)
	}

	username := os.Args[1]

	f, err := os.OpenFile("client_"+username+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		fmt.Println("Cannot log to seperate file")
		os.Exit(1)
	}

	defer f.Close()
    log.SetOutput(f)

	conn, err := grpc.NewClient("localhost:5000", grpc.WithTransportCredentials(insecure.NewCredentials()))
    
	if err != nil {
        log.Fatalf("[Client] [Error] Cannot connect to ChitChat server: %v", err)
		
    }

	defer conn.Close()

	c := &client{
		username: username,
		grpcCli:  pb.NewChitChatClient(conn),
        done:     make(chan struct{}),
	}

	 log.Printf("[Client] [Startup] %s is online", c.username)

	c.lock.Lock()
    c.clock++
	cliTs := c.clock
    c.lock.Unlock()
    
	stream, err := c.grpcCli.Join(context.Background(), &pb.JoinRequest{
        Username:  c.username,
        Timestamp: cliTs,
    })

    if err != nil {
        log.Fatalf("[Client] [Error] %s could not join ChitChat: %v", c.username, err)
    }

    log.Printf("[Client] [Join] %s joined ChitChat at logical time %d", c.username, cliTs) 

    
    go c.receiveMessages(stream)

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, os.Interrupt)
    go func() {
        <-sigCh
        c.leave()
    }()

    
    c.readInput()
}

func (c *client) receiveMessages(stream grpc.ServerStreamingClient[pb.Broadcast]) { 
	defer close(c.done)

	for {
		msg, err := stream.Recv()

		if err != nil {
			if err == io.EOF {
				log.Printf("[Client] [Received] stream closed by server")
			} else {
				log.Printf("[Client] [Received] %s lost connection to ChitChat: %v", c.username, err)
				fmt.Println("Connection lost...")
			}
			return
		}

		c.lock.Lock()
		c.clock = max(c.clock, msg.GetTimestamp()) + 1
    	c.lock.Unlock()

		fmt.Printf("%s: Sent by %s at logical time %d \n", msg.GetMsgContent(), msg.GetUsername(), msg.GetTimestamp())

		log.Printf("[Client] [Received] %s received message: %s; From %s at logical time %d", c.username, msg.GetMsgContent(), msg.GetUsername(), msg.GetTimestamp())
	}
		
}  // dit "listen": Recv-løkke, vis + log

func (c *client) readInput() {
    scanner := bufio.NewScanner(os.Stdin)  

    for scanner.Scan() {                     // venter, indtil brugeren trykker Enter
        text := scanner.Text()

        if text == "/leave" {
            c.leave()
			return
        }

        c.publish(text)
    }
	c.leave()
}                 // dit "wait": tastatur-løkke, /leave eller publish

func (c *client) publish(text string) {
    if utf8.RuneCountInString(text) > 128 || text == "" {   // tom eller over 128 tegn (utf8.RuneCountInString)
        fmt.Println("Your message is invalid")
        log.Printf("[Client] [Publish] Message by %s is too long or empty", c.username)
        return
    }

    // ── ⏱ SEND ──
    c.lock.Lock()
    c.clock++
    sendTs := c.clock
    c.lock.Unlock()

    reply, err := c.grpcCli.Publish(context.Background(), &pb.PublishRequest{
        Username:   c.username,
        Timestamp:  sendTs,
        MsgContent: text,
    })
    if err != nil {
        fmt.Println("Your message could not be sent")
		log.Printf("[Client] [Publish] Sent message by %s failed: %v", c.username, err)
        return
    }

    // ── ⏱ MODTAG (svaret er en besked fra serveren) ──
    c.lock.Lock()
    c.clock = max(c.clock, reply.GetTimestamp()) + 1
    replyTs := c.clock
    c.lock.Unlock()

    log.Printf("[Client] [Publish] %s; Was sent by %s at logical time %d, confirmed at %d", text, c.username, sendTs, replyTs)

}         // valider → send-regel → Publish → modtag-regel → log

func (c *client) leave() {
    c.leaveOnce.Do(func() {   // alt inde i Do kører kun første gang leave() kaldes

        // ── ⏱ SEND ──
        c.lock.Lock()
        c.clock++
        sendTs := c.clock
        c.lock.Unlock()

        reply, err := c.grpcCli.Leave(context.Background(), &pb.LeaveRequest{
            Username:  c.username,
            Timestamp: sendTs,
        })

        if err != nil {
            log.Printf("[Client] [Leave] Leave by %s failed: %v", c.username, err)
        } else {
            // ── ⏱ MODTAG ──
            c.lock.Lock()
            c.clock = max(c.clock, reply.GetTimestamp()) + 1
            replyTs := c.clock
            c.lock.Unlock()

            log.Printf("[Client] [Leave] %s left ChitChat at logical time %d, confirmed %d", c.username, sendTs, replyTs)
        }

        <-c.done   // vent, til receiveMessages er helt færdig

        log.Printf("[Client] [Shutdown]: %s ", c.username)
        fmt.Println("You left ChitChat...")
        os.Exit(0)
    })
}                      // sync.Once → Leave → <-done → log → os.Exit(0)