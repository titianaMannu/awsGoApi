package main

import (
	"SDCC-A3-Project/rpcFunctions"
	"SDCC-A3-Project/snsManagement"
	"SDCC-A3-Project/sqsManagement"
	"SDCC-A3-Project/utilities"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"log"
	"net"
	"net/rpc"
	"time"
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
	snsManagement.SnsToSqsConfig(&s.QueueURL, &s.TopicARN)

	go func() { LookForMessages(s) }()

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

type UpdateInfo struct {
	ID     string   `json:"ID"`
	Topics []string `json:"Topics"`
}

func LookForMessages(s *rpcFunctions.Service) {
	// This function must be called in a thread/goroutine
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	var to int64
	to = 20
	for {
		msgResult, err := sqsManagement.GetMessages(sess, &s.QueueURL, &to)
		if err != nil {
			fmt.Println("Got an error receiving messages:")
			fmt.Println(err)
			return
		}
		if len(msgResult.Messages) == 0 {
			fmt.Println("no messages available")
			fmt.Printf("%v+\n", time.Now())
			time.Sleep(time.Minute)
			continue
		}

		fmt.Println((*msgResult.Messages[0]).String())
		var jsonResult map[string]string
		json.Unmarshal([]byte(*msgResult.Messages[0].Body), &jsonResult)

		data := jsonResult["Message"]
		var updates []UpdateInfo
		json.Unmarshal([]byte(data), &updates)
		fmt.Println(updates)

		//otherwise the message return visible after the visibility timeout
		err = sqsManagement.DeleteMessage(sess, &s.QueueURL, msgResult.Messages[0].ReceiptHandle)
		if err != nil {
			fmt.Println("Got an error deleting the message:")
			fmt.Println(err)
		}

		s.RwMtx.Lock()
		for i := 0; i < len(updates); i++ {
			// per ogni id dall'esterno vedo la lista corrispondente se esiste
			if myList, ok := s.UsersIdMap[updates[i].ID]; ok {
				//per ogni topic nella lista updates
				for j := 0; j < len(updates[i].Topics); j++ {
					//vedo se ne sono gi a conoscenza
					if !Contains(myList, updates[i].Topics[j]) {
						myList = append(myList, updates[i].Topics[j])
					}
				}
			} else {
				s.UsersIdMap[updates[i].ID] = updates[i].Topics
			}
		}
		s.RwMtx.Unlock()
	}
}

// Contains tells whether a contains x.
func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}
