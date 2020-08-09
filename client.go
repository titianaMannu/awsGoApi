package main

import (
	"AwsGoApiTest/queueManagement"
	"AwsGoApiTest/utilities"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

type Arguments struct {
	ID                string   `json:"user_id"`
	SubscribeTopics   []string `json:"subscribe_topics"`   // need to activate a subscription for these topics
	UnsubscribeTopics []string `json:"unsubscribe_topics"` // need to unsubscribe these topics
	Actions           []Item   `json:"actions"`            // GET, SEND
}

type Item struct {
	Action  string   `json:"action"`
	Topic   string   `json:"topic"`
	Message []string `json:"messages"`
	Number  int
}

var QueueURL = make(map[string]string)

func main() {
	// if the filename is not specified we use "prodA.json" as default
	//after build just use $./producer -h to retrieve usage's information
	filename := flag.String("json", "jsons/actions.json", "a json file")
	serverAddr := flag.String("addr", "localhost", "server ip address")
	serverPort := flag.Int("serverPort", utilities.ServerPort, "server port number")

	flag.Parse()
	var client *rpc.Client
	client = connectWithServer(*serverAddr, *serverPort)
	arguments := parseJsonFile(*filename)

	clientRoutine(client, arguments)

}

func clientRoutine(client *rpc.Client, args Arguments) {
	if len(args.ID) <= 0 { //need to do a registration
		args.ID = doRegistration(client)
	} //otherwise we already have a valid user id

	doSubscriptions(client, args)
	doActions(client, args)
	deleteSubscriptions(client, args)

}

func sendAMessage(URL *string, message *string, userId *string) {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	err := queueManagement.SendMsg(sess, URL, message, userId)
	if err != nil {
		fmt.Println("Got an error sending the message:")
		fmt.Println(err)
		return
	}

	fmt.Println("Sent message to queue ")
}

func getAMessage(URL *string, visibilityTO *int64) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	msgResult, err := queueManagement.GetMessages(sess, URL, visibilityTO)
	if err != nil {
		fmt.Println("Got an error receiving messages:")
		fmt.Println(err)
		return
	}
	if len(msgResult.Messages) == 0 {
		return
	}
	fmt.Println("Message ID:     " + *msgResult.Messages[0].MessageId)
	fmt.Println("Message Handle: " + *msgResult.Messages[0].ReceiptHandle)
	fmt.Println("Message Attributes: ")

	for key, element := range msgResult.Messages[0].MessageAttributes {
		fmt.Println("Key:", key, "=>", "Element:", element)
	}

	fmt.Println("Message Body: " + *msgResult.Messages[0].Body)
}

func deleteSubscriptions(client *rpc.Client, args Arguments) {
	for i := 0; i < len(args.UnsubscribeTopics); i++ { //iterate over subscription
		arg := utilities.RequestArg{ID: args.ID, Tag: args.UnsubscribeTopics[i]}
		var reply int
		err := client.Call("MessageService.DeleteSubscription", &arg, &reply)
		if err != nil {
			log.Fatal("error in DeleteSubscription: ", err)
		}
		//store urls into a map to not ask for a queue reference in a second time
		fmt.Printf("exit status: %d\n", reply)
	}
}

func doActions(client *rpc.Client, args Arguments) {
	for i := 0; i < len(args.Actions); i++ {
		current := args.Actions[i]
		arg := utilities.RequestArg{ID: args.ID, Tag: current.Topic}
		var URL string

		// url of  the queue retrieval
		if _, isPresent := QueueURL[current.Topic]; !isPresent {
			err := client.Call("MessageService.GetQueueURL", &arg, &URL)
			if err != nil {
				log.Fatal("error in GetQueueURL: ", err)
			}
			QueueURL[current.Topic] = URL
		}

		fmt.Println(QueueURL[current.Topic])
		url := QueueURL[current.Topic]
		if current.Action == "SEND" {
			for j := 0; j < len(current.Message); j++ {
				sendAMessage(&url, &current.Message[j], &args.ID)
			}
		} else if current.Action == "GET" {
			for j := 0; j < current.Number; j++ {
				var to int64
				to = 5
				getAMessage(&url, &to)
			}
		}
	}
}

func doSubscriptions(client *rpc.Client, args Arguments) {
	for i := 0; i < len(args.SubscribeTopics); i++ { //iterate over subscription
		arg := utilities.RequestArg{ID: args.ID, Tag: args.SubscribeTopics[i]}
		reply := new(utilities.SubscriptionOutput)
		err := client.Call("MessageService.MakeSubscriptionToTopic", &arg, reply)
		if err != nil {
			log.Fatal("error in MakeSubscriptionToTopic: ", err)
		}
		//store urls into a map to not ask for a queue reference in a second time
		QueueURL[args.SubscribeTopics[i]] = reply.QueueURL
		fmt.Println(reply.QueueURL)
	}
}

func doRegistration(client *rpc.Client) string {
	var replyID string
	arg := new(utilities.RequestArg)
	//blocking call.. we cannot do anything without a user id
	err := client.Call("MessageService.GenerateUserId", arg, &replyID)
	if err != nil {
		log.Fatal("error in GenerateUserId: ", err)
	}
	fmt.Println("user ID: " + replyID)
	return replyID
}

func connectWithServer(serverAddr string, serverPort int) *rpc.Client {
	// Try to connect to localhost:1234 (the serverPort on which RPC server is listening)
	serverRefer := fmt.Sprintf("%s:%d", serverAddr, serverPort)
	client, err := rpc.Dial("tcp", serverRefer)
	if err != nil {
		log.Fatal("Error in dialing: ", err)
	}
	return client
}

func parseJsonFile(filename string) Arguments {
	// Open our jsonFile
	jsonFile, err := os.Open(filename)
	// if we os.Open returns an error then handle it
	if err != nil {
		fmt.Println(err)
	}

	fmt.Println("Successfully Opened the file")
	// defer the closing of our jsonFile so that we can parse it later on
	defer jsonFile.Close()

	// read our opened xmlFile as a byte array.
	byteValue, _ := ioutil.ReadAll(jsonFile)

	// we initialize our Users array
	var arguments Arguments

	// we unmarshal our byteArray which contains our
	// jsonFile's content into 'users' which we defined above
	json.Unmarshal(byteValue, &arguments)

	return arguments
}
