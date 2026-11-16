package main

import (
	pb "handin5/auction"
)

type client struct {
	id int64 // klientens id. Bruges så auktionserveren fx. ved hvem der er highestBidder
	reqId int64 // id for bids. Skal bruges i tilfældet af at auktionserveren crasher og klienten sender samme bud
	
	auctionClient pb.AuctionClient // forbindelsen til auktionserveren som er leader.

	auctioneers []int64 // en liste af de noder klienten kan sende bids til
	currentAuctioneer int // nuværende leaders indeksposition i listen af noder. 
}

func main() {

}

// skal lave et bud til auctionServeren
func (c *client) makeBid(amount int64) {
	
}

// henter resultatet. hvem har højeste bud for nu, hvad er budet og om auktionen er slut
func (c *client) getResults() (results *pb.ResultReply){
	return &pb.ResultReply{}
}

// leaderen svarer ikke (er crashet) og auctionClient skal skabe forbindelse til en ny auctionServer
func (c *client) newAuctioneer() {

}