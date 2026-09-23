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

func fork1(chF1_P1, chF1_P5 chan string) {
	available := true

	for {
		select {
		case msg1 := <-chF1_P1:
			if msg1 == "I want to eat!" {
				if available {
					available = false
					chF1_P1 <- "Of course!"
				} else {
					chF1_P1 <- "Sorry, I am taken..."
				}
			} else if msg1 == "Thanks for the meal!" {
				available = true
			}

		case msg5 := <-chF1_P5:
			if msg5 == "I want to eat!" {
				if available {
					available = false
					chF1_P5 <- "Of course!"
				} else {
					chF1_P5 <- "Sorry, I am taken..."
				}
			} else if msg5 == "Thanks for the meal!" {
				available = true
			}
		}
	}
}

func fork2(chF2_P1, chF1_P2 chan string) {
	available := true

	for {
		select {
		case msg1 := <-chF2_P1:
			if msg1 == "I want to eat!" {
				if available {
					available = false
					chF2_P1 <- "Of course!"
				} else {
					chF2_P1 <- "Sorry, I am taken..."
				}
			} else if msg1 == "Thanks for the meal!" {
				available = true
			}

		case msg2 := <-chF1_P2:
			if msg2 == "I want to eat!" {
				if available {
					available = false
					chF1_P2 <- "Of course!"
				} else {
					chF1_P2 <- "Sorry, I am taken..."
				}
			} else if msg2 == "Thanks for the meal!" {
				available = true
			}
		}
	}
}
func fork3(chF3_P2, chF3_P3 chan string) {
	available := true

	for {
		select {
		case msg2 := <-chF3_P2:
			if msg2 == "I want to eat!" {
				if available {
					available = false
					chF3_P2 <- "Of course!"
				} else {
					chF3_P2 <- "Sorry, I am taken..."
				}
			} else if msg2 == "Thanks for the meal!" {
				available = true
			}

		case msg3 := <-chF3_P3:
			if msg3 == "I want to eat!" {
				if available {
					available = false
					chF3_P3 <- "Of course!"
				} else {
					chF3_P3 <- "Sorry, I am taken..."
				}
			} else if msg3 == "Thanks for the meal!" {
				available = true
			}
		}
	}
}
func fork4(chF3_P3, chF3_P4 chan string) {
	available := true

	for {
		select {
		case msg3 := <-chF3_P3:
			if msg3 == "I want to eat!" {
				if available {
					available = false
					chF3_P3 <- "Of course!"
				} else {
					chF3_P3 <- "Sorry, I am taken..."
				}
			} else if msg3 == "Thanks for the meal!" {
				available = true
			}

		case msg4 := <-chF3_P4:
			if msg4 == "I want to eat!" {
				if available {
					available = false
					chF3_P4 <- "Of course!"
				} else {
					chF3_P4 <- "Sorry, I am taken..."
				}
			} else if msg4 == "Thanks for the meal!" {
				available = true
			}
		}
	}
}
func fork5(chF5_P4, chF5_P5 chan string) {
	available := true

	for {
		select {
		case msg1 := <-chF5_P4:
			if msg1 == "I want to eat!" {
				if available {
					available = false
					chF5_P4 <- "Of course!"
				} else {
					chF5_P4 <- "Sorry, I am taken..."
				}
			} else if msg1 == "Thanks for the meal!" {
				available = true
			}

		case msg5 := <-chF5_P5:
			if msg5 == "I want to eat!" {
				if available {
					available = false
					chF5_P5 <- "Of course!"
				} else {
					chF5_P5 <- "Sorry, I am taken..."
				}
			} else if msg5 == "Thanks for the meal!" {
				available = true
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
	answer1 := <-chF1_P1

	if answer1 != "Of course!" {
		time.Sleep(2 * time.Second)
		philosopher1Loop(chF1_P1, chF2_P1, timesEaten)
		return
	}

	for {
		chF2_P1 <- "I want to eat!"
		if <-chF2_P1 == "Of course!" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("philosopher1: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF1_P1 <- "Thanks for the meal!"
	chF2_P1 <- "Thanks for the meal!"
	philosopher1Loop(chF1_P1, chF2_P1, timesEaten)
}

func philosopher2(chF1_P2, chF2_P2 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher2Loop(chF1_P2, chF2_P2, timesEaten)

}

func philosopher2Loop(chF1_P2, chF2_P2 chan string, timesEaten int) {
	fmt.Println("philosopher2: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF1_P2 <- "I want to eat!"
	answer1 := <-chF1_P2

	if answer1 != "Of course!" {
		time.Sleep(2 * time.Second)
		philosopher2Loop(chF1_P2, chF2_P2, timesEaten)
		return
	}

	for {
		chF2_P2 <- "I want to eat!"
		if <-chF2_P2 == "Of course!" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("philosopher2: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF1_P2 <- "Thanks for the meal!"
	chF2_P2 <- "Thanks for the meal!"
	philosopher2Loop(chF1_P2, chF2_P2, timesEaten)

}

func philosopher3(chF2_P3, chF3_P3 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher3Loop(chF2_P3, chF3_P3, timesEaten)
}

func philosopher3Loop(chF2_P3, chF3_P3 chan string, timesEaten int) {
	fmt.Println("philosopher3: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF2_P3 <- "I want to eat!"
	answer1 := <-chF2_P3

	if answer1 != "Of course!" {
		time.Sleep(2 * time.Second)
		philosopher3Loop(chF2_P3, chF3_P3, timesEaten)
		return
	}

	for {
		chF3_P3 <- "I want to eat!"
		if <-chF3_P3 == "Of course!" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("philosopher3: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF2_P3 <- "Thanks for the meal!"
	chF3_P3 <- "Thanks for the meal!"
	philosopher3Loop(chF2_P3, chF3_P3, timesEaten)
}

func philosopher4(chF3_P4, chF4_P4 chan string, timesEaten int, wg *sync.WaitGroup) {
	defer wg.Done()
	philosopher4Loop(chF3_P4, chF4_P4, timesEaten)
}

func philosopher4Loop(chF3_P4, chF4_P4 chan string, timesEaten int) {
	fmt.Println("philosopher4: I am thinking")

	if timesEaten >= 3 {
		return
	}

	chF3_P4 <- "I want to eat!"
	answer1 := <-chF3_P4

	if answer1 != "Of course!" {
		time.Sleep(2 * time.Second)
		philosopher4Loop(chF3_P4, chF4_P4, timesEaten)
		return
	}

	for {
		chF4_P4 <- "I want to eat!"
		if <-chF4_P4 == "Of course!" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("philosopher4: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF3_P4 <- "Thanks for the meal!"
	chF4_P4 <- "Thanks for the meal!"
	philosopher4Loop(chF3_P4, chF4_P4, timesEaten)
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

	// Philosopher 5 takes the forks in the opposite order from the others.
	// This breaks the circular wait while each philosopher keeps its first fork
	// and waits for the second one to become available.
	chF1_P5 <- "I want to eat!"
	if <-chF1_P5 != "Of course!" {
		time.Sleep(2 * time.Second)
		philosopher5Loop(chF1_P5, chF5_P5, timesEaten)
		return
	}

	for {
		chF5_P5 <- "I want to eat!"
		if <-chF5_P5 == "Of course!" {
			break
		}
		time.Sleep(2 * time.Second)
	}

	fmt.Println("philosopher5: *eating*")
	timesEaten++
	time.Sleep(3 * time.Second)
	chF5_P5 <- "Thanks for the meal!"
	chF1_P5 <- "Thanks for the meal!"
	philosopher5Loop(chF1_P5, chF5_P5, timesEaten)
}
