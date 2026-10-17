package main

import (
	"bufio"
	"context"
	"fmt"
	pb "gm/grpc"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)
	

type client struct {
    username string
	grpcCli pb.ChitChatClient
	lock sync.Mutex
	clock int64
	done chan struct{} // en tom kanal. bruges som signal til færdiggøre receiveMessages() inden klienten forsvinder helt med leave()
	leaveOnce sync.Once // sørger for at brugeren kun kan leave én gang
}

func main() {
	if len(os.Args) < 2 { // sikkerhedtjek hvis bruger ikke skriver sit navn i terminalen
		fmt.Println("Usage: go run ./client <username>")
		os.Exit(1)
	}

	username := os.Args[1] // brugerens indtastet navn

	f, err := os.OpenFile("client_"+username+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644) // Laver klientens log fil som skal bruges til dokumentation i rapporten

	if err != nil { // sikkerhed hvis log filen åbnes/oprettes
		fmt.Println("Cannot log to seperate file")
		os.Exit(1)
	}

	defer f.Close() // lukker filen når main er færdig
    log.SetOutput(f) // sørger for at ALT log indsættes i log filen

	conn, err := grpc.NewClient("localhost:5000", grpc.WithTransportCredentials(insecure.NewCredentials())) // klargør forbindelsen til serveren. Host har samme navn hos klienten som serveren
    
	if err != nil { // sikkerhedstjek hvis klienten ikke kan skabe forbindelse til server
        log.Fatalf("[Client] [Error] Cannot connect to ChitChat server: %v", err)
		
    }

	defer conn.Close() // lukker forbindelsen når main er færdig

	c := &client{ // laver klienten. 
		username: username,
		grpcCli:  pb.NewChitChatClient(conn),
        done:     make(chan struct{}),
	}

	 log.Printf("[Client] [Startup] %s is online", c.username)
    
    // At joine serveren er en hændelse så vi skal sende et timestamp med til serveren
	c.lock.Lock() // Låser således at andre goroutines ikke kan pille ved klientens felter før vi er færdige
    c.clock++ // tæller klients ur op
	cliTs := c.clock // gemmer klientens tid i lokal variabel
    c.lock.Unlock() // Låser op igen så andre goroutines kan bruge klientens felter
    
	stream, err := c.grpcCli.Join(context.Background(), &pb.JoinRequest{ // Brugeren anmoder om at joine serveren.
        Username:  c.username,
        Timestamp: cliTs,
    })

    if err != nil { // sikkerhed hvis join fejler
        log.Fatalf("[Client] [Error] %s could not join ChitChat: %v", c.username, err)
    }

    log.Printf("[Client] [Join] %s joined ChitChat at logical time %d", c.username, cliTs) // Join er succesfuld

    
    go c.receiveMessages(stream) // starter en goroutine så bruger kan modtage og læse beskeder fra andre brugere og serveren

    sigCh := make(chan os.Signal, 1) // Fanger nedlukning af klienten således at fx Ctrl+C "lukker pænt" 
    signal.Notify(sigCh, os.Interrupt) // og indsætter det i sigCh kanalen

    go func() { // lille anonym funktion. Denne funktion kører i baggrunden og venter bare på at noget kommer ind på sigCh kanalen
        <-sigCh
        c.leave() // kalder klientens leave metode således at en nedlukning af klienten stadigvæk forlader "pænt"
    }()

    
    c.readInput() // Denne metode sørger for at main() ikke slutter før brugeren skriver /leave. SE METODE LÆNGERE NEDE
}

func (c *client) receiveMessages(stream grpc.ServerStreamingClient[pb.Broadcast]) { 
	defer close(c.done) // signalere til leave når metoden er færdig

	for { // en uendelig løkke. sørger at bruger kan få beskeder. stopper kun hvis serveren lukker/bruger har forladt eller klient mister forbindelse
		msg, err := stream.Recv() // venter på besked fra serveren

		if err != nil { // sikkerhedstjek. Stopper hvis serveren lukker eller klient mister forbindelse
			if err == io.EOF {
				log.Printf("[Client] [Received] stream closed")
			} else {
				log.Printf("[Client] [Received] %s lost connection to ChitChat: %v", c.username, err)
				fmt.Println("Connection lost...")
			}
			return
		}

        // Brugeren modtager en besked. Vi opdaterer klientens ur
		c.lock.Lock()
		c.clock = max(c.clock, msg.GetTimestamp()) + 1
    	c.lock.Unlock()

		fmt.Printf("%s: Sent by %s at logical time %d \n", msg.GetMsgContent(), msg.GetUsername(), msg.GetTimestamp()) // bliver vist il brugeren i terminalen

		log.Printf("[Client] [Received] %s received message: %s; From %s at logical time %d", c.username, msg.GetMsgContent(), msg.GetUsername(), msg.GetTimestamp()) // logges til log filen
	}	
}  

func (c *client) readInput() { // læser det brugeren skriver
    scanner := bufio.NewScanner(os.Stdin) // laver en scanner som skal læse brugerens input

    for scanner.Scan() { // scan() returnere en boolean. når den er falsk stopper input, for loopet bliver afbrudt og vi kalder leave()
        text := scanner.Text() // gemmer den scannede tekst brugeren skriver

        if text == "/leave" { // hvis brugeren skriver /leave forlader han/hun chatten
            c.leave()
			return
        }

        c.publish(text) // Udgiver det brugeren har skrevet
    }
	c.leave() // kalder leave når for løkken er færdig
}                

func (c *client) publish(text string) {
    if utf8.RuneCountInString(text) > 128 || text == "" {   // Hvis brugerens besked er tom eller over 128 tegn afvises den
        fmt.Println("Your message is invalid")
        log.Printf("[Client] [Publish] Message by %s is too long or empty", c.username)
        return
    }

    // Brugeren sender en besked. Vi gemmer klientens ur som skal medsendes i requesten
    c.lock.Lock()
    c.clock++
    sendTs := c.clock
    c.lock.Unlock()

    reply, err := c.grpcCli.Publish(context.Background(), &pb.PublishRequest{ // brugeren sender sin besked til serveren
        Username:   c.username,
        Timestamp:  sendTs,
        MsgContent: text,
    })

    if err != nil { // hvis beskeden mislykkedes at blive sendt
        fmt.Println("Your message could not be sent")
		log.Printf("[Client] [Publish] Sent message by %s failed: %v", c.username, err)
        return
    }

    // serveren sender et svar tilbage. Derfor skal vi opdatere klientens ur
    c.lock.Lock()
    c.clock = max(c.clock, reply.GetTimestamp()) + 1
    replyTs := c.clock
    c.lock.Unlock()

    log.Printf("[Client] [Publish] %s; Was sent by %s at logical time %d, confirmed at %d", text, c.username, sendTs, replyTs) // vi logger hændelsen i log filen
}   

func (c *client) leave() {
    c.leaveOnce.Do(func() {   // alt inde i Do kører kun første gang leave() kaldes. sikrer, at indholdet kun kører én gang

        // vi laver en hændelse. vi opdatere klientens ur
        c.lock.Lock()
        c.clock++
        sendTs := c.clock
        c.lock.Unlock()

        // vi beder serveren om at fjerne brugeren
        reply, err := c.grpcCli.Leave(context.Background(), &pb.LeaveRequest{
            Username:  c.username,
            Timestamp: sendTs,
        })

        if err != nil { 
            log.Printf("[Client] [Leave] Leave by %s failed: %v", c.username, err) // Leave fejlede fx fordi serveren er nede.
        } else {
            // vi opdatere klients ur, da det at serveren fjerner brugeren er en hændelse fra serveren.
            c.lock.Lock()
            c.clock = max(c.clock, reply.GetTimestamp()) + 1
            replyTs := c.clock
            c.lock.Unlock()

            log.Printf("[Client] [Leave] %s left ChitChat at logical time %d, confirmed %d", c.username, sendTs, replyTs) // vi logger en bekræftelse på har bruger forladt
        }

        <-c.done   // venter, til receiveMessages er helt færdig

        // logger til log filen og skriver til brugeren at de har forladt
        log.Printf("[Client] [Shutdown]: %s ", c.username) 
        fmt.Println("You left ChitChat...")
        os.Exit(0)
    })
}                   