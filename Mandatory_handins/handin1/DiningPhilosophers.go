package main

import (
	"fmt"
	"time"
	"sync"
)


// Fork represents a fork in the dining philosophers problem
type Fork struct {
	aquire chan chan bool
	release chan struct{}
}
// fork is a goroutine that manages the state of a fork
func fork(aquire <-chan chan bool, release <-chan struct{}) {
	available := true
	// The fork goroutine continuously listens for acquire and release requests
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
// tryAquire attempts to acquire a fork and returns true if successful, false otherwise
func tryAquire(fork Fork) bool {
	reply := make(chan bool)
	fork.aquire <- reply
	return <-reply
}

func releaseFork(fork Fork) {
	fork.release <- struct{}{}
}

// philosopher simulates a philosopher's behavior of thinking and eating
func philosopher(name string, left Fork, right Fork, wg *sync.WaitGroup) {
	defer wg.Done()

	eaten := 0
	// Each philosopher will eat 3 times
	for eaten < 3 {
		fmt.Println(name, "is thinking")
		time.Sleep(100 * time.Millisecond)

		first := left
		second := right
// To avoid deadlock, Philosopher 5 picks up the right fork first
		if name == "Philosopher 5" {
			first = right
			second = left
		}
// Try to acquire the first fork
		if !tryAquire(first) {
			continue
		}
// If the first fork is acquired, try to acquire the second fork
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
// main function initializes the forks and philosophers, and starts the simulation
func main() {
	forks := make([]Fork, 5)
// Initialize the forks and start their goroutines
	for i := range forks {
		forks[i] = Fork{
			aquire: make(chan chan bool),
			release: make(chan struct{}),
		}
		go fork(forks[i].aquire, forks[i].release)
	}
// WaitGroup to wait for all philosophers to finish
	var wg sync.WaitGroup
	wg.Add(5)
// Start the philosophers as goroutines
	go philosopher("Philosopher 1", forks[0], forks[1], &wg)
	go philosopher("Philosopher 2", forks[1], forks[2], &wg)
	go philosopher("Philosopher 3", forks[2], forks[3], &wg)
	go philosopher("Philosopher 4", forks[3], forks[4], &wg)
	go philosopher("Philosopher 5", forks[4], forks[0], &wg)

	wg.Wait()
	fmt.Println("All philosophers are done eating")
}
