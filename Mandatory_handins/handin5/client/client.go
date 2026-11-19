package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "handin5/auction"
)

const (
	maxAttempts = 10                     // forsøg per kald, på tværs af noder
	retryDelay  = 500 * time.Millisecond // pause mellem forsøg. Giver backup tid til at overtage
	rpcTimeout  = 3 * time.Second
)

type client struct {
	id    int64 // klientens id. Bruges så auktionserveren fx. ved hvem der er highestBidder. Må ikke være 0
	reqId int64 // id for bids. Skal bruges i tilfældet af at auktionserveren crasher og klienten sender samme bud

	conn          *grpc.ClientConn
	auctionClient pb.AuctionClient // forbindelsen til auktionserveren som er leader.

	currentAuctioneer int // nuværende leaders indeksposition i addrs.
}

var addrs = []string{
	"localhost:5000",
	"localhost:5001",
}

func main() {
	id := flag.Int64("id", 1, "id of this client (must not be 0)")
	flag.Parse()

	logFile, err := os.OpenFile(fmt.Sprintf("client%d.log", *id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	c := &client{id: *id}
	c.connect()
	defer func() { c.conn.Close() }()

	for {
		res := c.getResults()
		if res == nil {
			log.Printf("[Client %d] [Error] No auctioneer reachable. Giving up", c.id)
			return
		}
		if res.Over {
			log.Printf("[Client %d] [Auction] Auction is over. Highest bidder: %d, Amount: %d", c.id, res.HighestBidder, res.Amount)
			return
		}
		if res.HighestBidder == c.id {
			log.Printf("[Client %d] [Auction] I am currently the highest bidder with amount: %d", c.id, res.Amount)
			time.Sleep(200 * time.Millisecond) // sleep lidt for at give andre klienter en chance for at byde
			continue
		}
		log.Printf("[Client %d] [Auction] Current highest bidder: %d, Amount: %d", c.id, res.HighestBidder, res.Amount)
		c.makeBid(res.Amount + 1 + rand.Int63n(10)) // byder lidt mere end det nuværende højeste bud
		time.Sleep(time.Duration(100+rand.Intn(400)) * time.Millisecond)
	}
}

// opretter forbindelse til den nuværende auctioneer
func (c *client) connect() {
	if c.conn != nil {
		c.conn.Close()
	}
	conn, err := grpc.NewClient(addrs[c.currentAuctioneer], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Client %d] [Error] Cannot create connection to %s: %v", c.id, addrs[c.currentAuctioneer], err)
	}
	c.conn = conn
	c.auctionClient = pb.NewAuctionClient(conn)
}

// skal lave et bud til auctionServeren
func (c *client) makeBid(amount int64) {
	c.reqId++ // hvert bud skal have et unikt reqId. Bruges hvis leader crasher og klienten sender samme bud igen.
	req := &pb.BidRequest{ClientId: c.id, ReqId: c.reqId, Amount: amount}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		rep, err := c.auctionClient.Bid(ctx, req) // samme req ved alle forsøg, så dubletter genkendes
		cancel()

		if err == nil {
			log.Printf("[Client %d] [Bid] Bid made: %d, ack: %v", c.id, amount, rep.Ack)
			return
		}

		log.Printf("[Client %d] [Error] Bid attempt %d failed: %v", c.id, attempt, err)
		c.newAuctioneer()
		time.Sleep(retryDelay)
	}
	log.Printf("[Client %d] [Error] Bid %d could not be delivered", c.id, amount)
}

// henter resultatet. hvem har højeste bud for nu, hvad er budet og om auktionen er slut
// returnerer nil hvis ingen node kan nås
func (c *client) getResults() *pb.ResultReply {
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		res, err := c.auctionClient.Result(ctx, &pb.ResultRequest{})
		cancel()

		if err == nil {
			return res
		}

		log.Printf("[Client %d] [Error] Result attempt %d failed: %v", c.id, attempt, err)
		c.newAuctioneer()
		time.Sleep(retryDelay)
	}
	return nil
}

// leaderen svarer ikke (er crashet) og auctionClient skal skabe forbindelse til en ny auctionServer
func (c *client) newAuctioneer() {
	c.currentAuctioneer = (c.currentAuctioneer + 1) % len(addrs)
	c.connect()
}