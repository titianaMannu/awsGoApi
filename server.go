package main

import (
	"AwsGoApiTest/rpcFunctions"
	"AwsGoApiTest/utilities"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
)

func main() {
	// Program Parameters
	serverPort := flag.Int("serverPort", utilities.ServerPort, "a port number")
	flag.Parse()

	initServer(serverPort)

}

func initServer(serverPort *int) {

	// Queue Initialization
	s := new(rpcFunctions.Service)
	s.URLQueueMap = make(map[string]string)
	s.UsersIdMap = make(map[string][]string)
	s.QueueSubscribersMap = make(map[string]int)

	// Register a new rpc server and the struct we created above.
	server := rpc.NewServer()
	err := server.RegisterName("MessageService", s)
	if err != nil {
		log.Fatal("[CRITICAL] - Format of service Queue is not correct: ", err)
	}
	completeAddr := fmt.Sprintf(":%d", *serverPort)

	// Listen for incoming tcp packets on specified port.
	l, e := net.Listen("tcp", completeAddr)
	if e != nil {
		log.Fatal("[CRITICAL] - Listen error:", e)
	}

	log.Printf("[INFO] - server up and running. Listening port number: %d", *serverPort)
	// Link rpc server to the socket, and allow rpc server to accept
	// rpc requests coming from that socket.
	server.Accept(l)
}
