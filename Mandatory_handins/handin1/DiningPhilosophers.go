package main

import (
	"fmt"
	"time"
)

func main() {
	chP1 := make(chan string)
	chP2 := make(chan string)
	chP3 := make(chan string)
	chP4 := make(chan string)
	chP5 := make(chan string)
	chF1 := make(chan bool)
	chF2 := make(chan bool)
	chF3 := make(chan bool)
	chF4 := make(chan bool)
	chF5 := make(chan bool)

}

func fork1(chP1, chP5 chan string, chF1 chan bool) {
	chF1 <- true
}

func fork2(chP1, chP2 chan string, chF2 chan bool) {
	chF2 <- true
}

func fork3(chP3, chP2 chan string, chF3 chan bool) {
	chF3 <- true
}

func fork4(chP4, chP3 chan string, chF4 chan bool) {
	chF4 <- true
}

func fork5(chP4, chP5 chan string, chF5 chan bool) {
	chF5 <- true
}

func philosopher1(chF1, chF2 chan bool, chP1 chan string) {
	chP1 <- "I am thinking!"
	fmt.Println("philosopher1: " + chP1)
	
	chP1 <- "I am eating"

}

func philosopher2(chF3, chF2 chan bool, chP2 chan string) {
	chP2 <- "I am thinking!"
	fmt.Println("philosopher2: " + chP2)

}

func philosopher3(chF3, chF4 chan bool, chP3 chan string) {
	chP3 <- "I am thinking!"
	fmt.Println("philosopher3: " + chP3)
}

func philosopher4(chF5, chF4 chan bool, chP4 chan string) {
	chP4 <- "I am thinking!"
	fmt.Println("philosopher4: " + chP4)
}

func philosopher5(chF5, chF1 chan bool, chP5 chan string, timesEaten int) {
	chP5 <- "I am thinking!"
	fmt.println("philosopher5: " + chP5)
	if timesEaten >= 3 {
		return
	}
	if chF5 == true && chF1 == true {
		chF5 <- false
		chF1 <- false
		chP5 <- "I am eating!"
		fmt.Println("philosopher5: " + chP5)
		fmt.Println("philosopher5: *eating*")
		timesEaten++
		time.sleep(3 * time.Second)
		chF5 <- true
		chF1 <- true
		philosopher5(chF5, chF1 chan bool, chP5 chan string, timesEaten int)
	}
}
