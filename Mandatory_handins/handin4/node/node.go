package main

import (
	"sync"

	proto "gm/grpc"

	// "google.golang.org/grpc"
	// "google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	proto.UnimplementedRicartAgrawalaServer

	id int64
	port string

	lock sync.Mutex 
	clock int64
	requestTS int64 

	peers map[int64]proto.RicartAgrawalaClient 
}

func main() {
	
}