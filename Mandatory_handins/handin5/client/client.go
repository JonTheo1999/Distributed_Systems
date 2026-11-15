package main

import (
	pb "handin5/auction"
)

type client struct {
	id int64 
	reqId int64
	
	auctionClient pb.AuctionClient

	auctioneers []int64
	currentIndex int
}

func main() {

}

func makeBid() {
	
}

func getResults() {

}

func newAuctioneer() {

}