package main

import (
	"sync"
	"fmt"
)

func main() {
	ch := make(chan Message)
	var wg sync.WaitGroup
	wg.Add(2)

	go client(ch, &wg)
	go server(ch, &wg)

	wg.Wait()
	fmt.Println("Connection Established: Sending Data...")
}

func client(ch chan Message, wg *sync.WaitGroup) {
	msg := Message{From: "client", Type: SYN, Seq: 100, Ack: 0}
	ch <- msg
	answerFromServer := <-ch

	if answerFromServer.From == "server" {
		connect := Message{From: "client", Type: ACK, Seq: msg.Seq + 1, Ack: answerFromServer.Seq + 1}
		ch <- connect
	}

	wg.Done()
}

func server(ch chan Message, wg *sync.WaitGroup) {
	call := <-ch

	if call.From == "client" {
		msg := Message{From: "server", Type: SYNACK, Seq: 300, Ack: call.Seq + 1}
		ch <- msg
		_ = <- ch // connection established
	}

	wg.Done()
}

type MessageType int

const (
	SYN    MessageType = 0
	SYNACK MessageType = 1
	ACK    MessageType = 2
	DATA   MessageType = 3
)

type Message struct {
	From string
	Type MessageType
	Seq  int
	Ack  int
}
