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

type client struct {
	id    int64 // klientens id. Bruges så auktionserveren fx. ved hvem der er highestBidder
	reqId int64 // id for bids. Skal bruges i tilfældet af at auktionserveren crasher og klienten sender samme bud

	auctionClient pb.AuctionClient // forbindelsen til auktionserveren som er leader.

	auctioneers       []int64 // en liste af de noder klienten kan sende bids til
	currentAuctioneer int     // nuværende leaders indeksposition i listen af noder.
}

var addrs = []string{
	"localhost:5000",
	"localhost:5001",
}

func main() {
	id := flag.Int64("id", 1, "id of this client")
	flag.Parse()

	logFile, err := os.OpenFile(fmt.Sprintf("client%d.log", *id), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("could not open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	conn, err := grpc.NewClient(addrs[0], grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("[Client] [Error] Cannot connect to Auction server: %v", err)
	}
	defer conn.Close()

	c := &client{
		id:            *id,
		auctionClient: pb.NewAuctionClient(conn),
	}

	for {
		res := c.getResults()
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

// skal lave et bud til auctionServeren
func (c *client) makeBid(amount int64) {
	c.reqId++ // hvert bud skal have et unikt reqId. Bruges hvis leader crasher og klienten sender samme bud igen.

	rep, err := c.auctionClient.Bid(context.Background(), &pb.BidRequest{
		ClientId: c.id,
		ReqId:    c.reqId,
		Amount:   amount,
	})
	if err != nil {
		log.Printf("[Client %d] [Error] Failed to make bid: %v", c.id, err)
		return
	}
	log.Printf("[Client %d] [Bid] Bid made: %d, ack: %v", c.id, amount, rep.Ack)
}

// henter resultatet. hvem har højeste bud for nu, hvad er budet og om auktionen er slut
func (c *client) getResults() *pb.ResultReply {
	res, err := c.auctionClient.Result(context.Background(), &pb.ResultRequest{})
	if err != nil {
		log.Printf("[Client %d] [Error] Failed to get results: %v", c.id, err)
		return &pb.ResultReply{}
	}
	return res
}

// leaderen svarer ikke (er crashet) og auctionClient skal skabe forbindelse til en ny auctionServer
func (c *client) newAuctioneer() {

}
