package main

import (
	"fmt"
	"time"
	"sync"
)


type Fork struct {
	aquire chan chan bool
	release chan struct{}
}

func fork(aquire <-chan chan bool, release <-chan struct{}) {
	available := true
	for {
		select {
		case reply := <-aquire:
			if available {
				available = false
				reply <- true
			} else {
				reply <- false
			}
		case <-release:
			available = true
		}
	}
}

func tryAquire(fork Fork) bool {
	reply := make(chan bool)
	fork.aquire <- reply
	return <-reply
}
func releaseFork(fork Fork) {
	fork.release <- struct{}{}
}

func philosopher(name string,
	left Fork,
	right Fork,
	wg *sync.WaitGroup) {
	defer wg.Done()

	eaten := 0
	for eaten < 3 {
		fmt.Println(name, "is thinking")
		time.Sleep(100 * time.Millisecond)

		first := left
		second := right

		if name == "Philosopher 5" {
			first = right
			second = left
		}

		if !tryAquire(first) {
			continue
		}

		if !tryAquire(second) {
			releaseFork(first)
			time.Sleep(50 * time.Millisecond)
			continue
		}

		fmt.Println(name, "is eating")
		time.Sleep(150 * time.Millisecond)

		releaseFork(second)
		releaseFork(first)

		eaten++
		fmt.Println(name, "has finished eating", eaten, "times")
	}

	fmt.Println(name, "is done eating")

}

func main() {
	forks := make([]Fork, 5)

	for i := range forks {
		forks[i] = Fork{
			aquire: make(chan chan bool),
			release: make(chan struct{}),
		}
		go fork(forks[i].aquire, forks[i].release)
	}

	var wg sync.WaitGroup
	wg.Add(5)

	go philosopher("Philosopher 1", forks[0], forks[1], &wg)
	go philosopher("Philosopher 2", forks[1], forks[2], &wg)
	go philosopher("Philosopher 3", forks[2], forks[3], &wg)
	go philosopher("Philosopher 4", forks[3], forks[4], &wg)
	go philosopher("Philosopher 5", forks[4], forks[0], &wg)

	wg.Wait()
	fmt.Println("All philosophers are done eating")
}
