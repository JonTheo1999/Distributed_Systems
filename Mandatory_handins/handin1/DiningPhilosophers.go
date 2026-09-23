package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var wg sync.WaitGroup

	chF1_P1 := make(chan string)
	chF1_P5 := make(chan string)

	chF2_P1 := make(chan string)
	chF2_P2 := make(chan string)

	chF3_P2 := make(chan string)
	chF3_P3 := make(chan string)

	chF4_P3 := make(chan string)
	chF4_P4 := make(chan string)

	chF5_P4 := make(chan string)
	chF5_P5 := make(chan string)

	go fork1(chF1_P1, chF1_P5)
	go fork2(chF2_P1, chF2_P2)
	go fork3(chF3_P2, chF3_P3)
	go fork4(chF4_P3, chF4_P4)
	go fork5(chF5_P4, chF5_P5)

	wg.Add(5)
	go philosopher1(chF1_P1, chF2_P1, 0, &wg)
	go philosopher2(chF2_P2, chF3_P2, 0, &wg)
	go philosopher3(chF3_P3, chF4_P3, 0, &wg)
	go philosopher4(chF4_P4, chF5_P4, 0, &wg)
	go philosopher5(chF1_P5, chF5_P5, 0, &wg)

	wg.Wait()
	fmt.Println("All philosophers have finished eating")

}

// FORK DESIGN (identical for fork1–fork5):
// Each fork tracks whether it is currently free with the "available" variable and whether it´s
// neighbors are waiting with the "pendingP_" variable. When a
// fork is busy, it does NOT reject the requesting philosopher. Instead it records the
// request as pending and sends no reply. The philosopher's receive (<-chX_PY)
// therefore blocks until the fork later becomes free and actively sends
// "Of course!" to the waiting neighbor (see the "Thanks for the meal!" case).
// This implements philosophers HOLDING a fork while WAITING for the other.
// Coffman's "hold and wait" condition IS present in this design.
// Deadlock is instead prevented by breaking the "circular wait" condition.
// see the comment in philosopher5Loop.

// philosopher1–philosopher4 all request their forks in the same fixed order
// (their lower-numbered neighbor fork first, then the higher-numbered one).
// Because forks now implement hold-and-wait, a fully symmetric ring where 
// everyone requests forks in the same rotational order risks circular wait. 
// philosopher5 breaks this symmetry by requesting in the opposite order.
// See philosopher5Loop for why that prevents deadlock.

func fork1(chF1_P1, chF1_P5 chan string) {
	available := true
	pendingP1 := false 
	pendingP5 := false

	for {
		select {
		case msg1 := <-chF1_P1:
			if msg1 == "I want to eat!" {
				if available {
					available = false
					chF1_P1 <- "Of course!"
				} else {
					//chF1_P1 <- "Sorry, I am taken..."
					pendingP1 = true
				}
			} else if msg1 == "Thanks for the meal!" {
				if pendingP1 {
					pendingP1 = false
					chF1_P1 <- "Of course!"
				} else if pendingP5 {
					pendingP5 = false
					chF1_P5 <- "Of course!"
				} else {
					available = true
				}
			}

		case msg5 := <-chF1_P5:
			if msg5 == "I want to eat!" {
				if available {
					available = false
					chF1_P5 <- "Of course!"
				} else {
					//chF1_P5 <- "Sorry, I am taken..."
					pendingP5 = true
				}
			} else if msg5 == "Thanks for the meal!" {
				if pendingP5 {
					pendingP5 = false
					chF1_P5 <- "Of course!"
				} else if pendingP1 {
					pendingP1 = false
					chF1_P1 <- "Of course!"
				} else {
					available = true
				}
			}
		}
	}
}

func fork2(chF2_P1, chF2_P2 chan string) {
	available := true
	pendingP1 := false 
	pendingP2 := false

	for {
		select {
		case msg1 := <-chF2_P1:
			if msg1 == "I want to eat!" {
				if available {
					available = false
					chF2_P1 <- "Of course!"
				} else {
					pendingP1 = true
				}
			} else if msg1 == "Thanks for the meal!" {
				if pendingP1 {
					pendingP1 = false
					chF2_P1 <- "Of course!"
				} else if pendingP2 {
					pendingP2 = false
					chF2_P2 <- "Of course!"
				} else {
					available = true
				}
			}

		case msg2 := <-chF2_P2:
			if msg2 == "I want to eat!" {
				if available {
					available = false
					chF2_P2 <- "Of course!"
				} else {
					pendingP2 = true
				}
			} else if msg2 == "Thanks for the meal!" {
				if pendingP2 {
					pendingP2 = false
					chF2_P2 <- "Of course!"
				} else if pendingP1 {
					pendingP1 = false
					chF2_P1 <- "Of course!"
				} else {
					available = true
				}
			}
		}
	}
}
func fork3(chF3_P2, chF3_P3 chan string) {
	available := true
	pendingP2 := false
	pendingP3 := false

	for {
		select {
		case msg2 := <-chF3_P2:
			if msg2 == "I want to eat!" {
				if available {
					available = false
					chF3_P2 <- "Of course!"
				} else {
					pendingP2 = true
				}
			} else if msg2 == "Thanks for the meal!" {
				if pendingP2 {
					pendingP2 = false
					chF3_P2 <- "Of course!"
				} else if pendingP3 {
					pendingP3 = false
					chF3_P3 <- "Of course!"
				} else {
					available = true
				}
			}

		case msg3 := <-chF3_P3:
			if msg3 == "I want to eat!" {
				if available {
					available = false
					chF3_P3 <- "Of course!"
				} else {
					pendingP3 = true
				}
			} else if msg3 == "Thanks for the meal!" {
				if pendingP3 {
					pendingP3 = false
					chF3_P3 <- "Of course!"
				} else if pendingP2 {
					pendingP2 = false
					chF3_P2 <- "Of course!"
				} else {
					available = true
				}
			}
		}
	}
}

func fork4(chF4_P3, chF4_P4 chan string) {
	available := true
	pendingP3 := false
	pendingP4 := false

	for {
		select {
		case msg3 := <-chF4_P3:
			if msg3 == "I want to eat!" {
				if available {
					available = false
					chF4_P3 <- "Of course!"
				} else {
					pendingP3 = true
				}
			} else if msg3 == "Thanks for the meal!" {
				if pendingP3 {
					pendingP3 = false
					chF4_P3 <- "Of course!"
				} else if pendingP4 {
					pendingP4 = false
					chF4_P4 <- "Of course!"
				} else {
					available = true
				}
			}

		case msg4 := <-chF4_P4:
			if msg4 == "I want to eat!" {
				if available {
					available = false
					chF4_P4 <- "Of course!"
				} else {
					pendingP4 = true
				}
			} else if msg4 == "Thanks for the meal!" {
				if pendingP4 {
					pendingP4 = false
					chF4_P4 <- "Of course!"
				} else if pendingP3 {
					pendingP3 = false
					chF4_P3 <- "Of course!"
				} else {
					available = true
				}
			}
		}
	}
}

func fork5(chF5_P4, chF5_P5 chan string) {
	available := true
	pendingP4 := false
	pendingP5 := false

	for {
		select {
		case msg4 := <-chF5_P4:
			if msg4 == "I want to eat!" {
				if available {
					available = false
					chF5_P4 <- "Of course!"
				} else {
					pendingP4 = true
				}
			} else if msg4 == "Thanks for the meal!" {
				if pendingP4 {
					pendingP4 = false
					chF5_P4 <- "Of course!"
				} else if pendingP5 {
					pendingP5 = false
					chF5_P5 <- "Of course!"
				} else {
					available = true
				}
			}

		case msg5 := <-chF5_P5:
			if msg5 == "I want to eat!" {
				if available {
					available = false
					chF5_P5 <- "Of course!"
				} else {
					pendingP5 = true
				}
			} else if msg5 == "Thanks for the meal!" {
				if pendingP5 {
					pendingP5 = false
					chF5_P5 <- "Of course!"
				} else if pendingP4 {
					pendingP4 = false
					chF5_P4 <- "Of course!"
				} else {
					available = true
				}
			}
		}
	}
}

func philosopher1(chF1_P1, chF2_P1 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher1Loop(chF1_P1, chF2_P1, timesEaten)
}

func philosopher1Loop(chF1_P1, chF2_P1 chan string, timesEaten int) {
	fmt.Println("philosopher1: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF1_P1 <- "I want to eat!"
	<-chF1_P1        

	chF2_P1 <- "I want to eat!"
	<-chF2_P1

	fmt.Println("philosopher1: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF1_P1 <- "Thanks for the meal!"
	chF2_P1 <- "Thanks for the meal!"

	philosopher1Loop(chF1_P1, chF2_P1, timesEaten)
}

func philosopher2(chF2_P2, chF3_P2 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher2Loop(chF2_P2, chF3_P2, timesEaten)
}

func philosopher2Loop(chF2_P2, chF3_P2 chan string, timesEaten int) {
	fmt.Println("philosopher2: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF2_P2 <- "I want to eat!"
	<-chF2_P2

	chF3_P2 <- "I want to eat!"
	<-chF3_P2

	fmt.Println("philosopher2: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF2_P2 <- "Thanks for the meal!"
	chF3_P2 <- "Thanks for the meal!"

	philosopher2Loop(chF2_P2, chF3_P2, timesEaten)
}

func philosopher3(chF3_P3, chF4_P3 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher3Loop(chF3_P3, chF4_P3, timesEaten)
}

func philosopher3Loop(chF3_P3, chF4_P3 chan string, timesEaten int) {
	fmt.Println("philosopher3: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF3_P3 <- "I want to eat!"
	<-chF3_P3

	chF4_P3 <- "I want to eat!"
	<-chF4_P3

	fmt.Println("philosopher3: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF3_P3 <- "Thanks for the meal!"
	chF4_P3 <- "Thanks for the meal!"

	philosopher3Loop(chF3_P3, chF4_P3, timesEaten)
}

func philosopher4(chF4_P4, chF5_P4 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher4Loop(chF4_P4, chF5_P4, timesEaten)
}

func philosopher4Loop(chF4_P4, chF5_P4 chan string, timesEaten int) {
	fmt.Println("philosopher4: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF4_P4 <- "I want to eat!"
	<-chF4_P4

	chF5_P4 <- "I want to eat!"
	<-chF5_P4

	fmt.Println("philosopher4: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF4_P4 <- "Thanks for the meal!"
	chF5_P4 <- "Thanks for the meal!"

	philosopher4Loop(chF4_P4, chF5_P4, timesEaten)
}

func philosopher5(chF1_P5, chF5_P5 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher5Loop(chF1_P5, chF5_P5, timesEaten)
}

func philosopher5Loop(chF1_P5, chF5_P5 chan string, timesEaten int) {
	fmt.Println("philosopher5: I am thinking")

	if timesEaten >= 3 {
		return
	}

	// NOTE: order swapped relative to philosopher1–4 (asks for fork5 before fork1).
	// This asymmetry breaks circular wait.
	// PREVENTS DEADLOCK
	chF5_P5 <- "I want to eat!"
	<-chF5_P5

	chF1_P5 <- "I want to eat!"
	<-chF1_P5

	fmt.Println("philosopher5: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF5_P5 <- "Thanks for the meal!"
	chF1_P5 <- "Thanks for the meal!"

	philosopher5Loop(chF1_P5, chF5_P5, timesEaten)
}
