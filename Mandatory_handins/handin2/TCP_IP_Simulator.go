package main

import (
	"fmt"
	"sync"
)

func main() {
	ch := make(chan Message)
	var wg sync.WaitGroup
	wg.Add(2)

	go client(ch, &wg)
	go server(ch, &wg)

	wg.Wait()
	fmt.Println("Data sent!")
}

func client(ch chan Message, wg *sync.WaitGroup) {
	msg := Message{From: "client", Type: SYN, Seq: 100, Ack: 0}
	ch <- msg
	answerFromServer := <-ch

	if answerFromServer.From == "server" {
		connect := Message{From: "client", Type: ACK, Seq: msg.Seq + 1, Ack: answerFromServer.Seq + 1}
		ch <- connect
		data := Message{From: "client", Type: DATA, Seq: msg.Seq + 1, Ack: answerFromServer.Seq + 1}
		ch <- data
	}

	

	wg.Done()
}

func server(ch chan Message, wg *sync.WaitGroup) {
	call := <-ch

	if call.From == "client" {
		msg := Message{From: "server", Type: SYNACK, Seq: 300, Ack: call.Seq + 1}
		ch <- msg
		connect := <-ch // connection established
		fmt.Println("Connection Established: Sending Data:", connect)
		receivedData := <-ch
		fmt.Println("Server received data:", receivedData)
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
