package rpcFunctions

import (
	"SDCC-A3-Project/imports/shortuuid-master"
	"SDCC-A3-Project/snsManagement"

	"SDCC-A3-Project/sqsManagement"
	"SDCC-A3-Project/utilities"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"sync"
)

type Service struct {
	UsersIdMap          map[string][]string // topicString:List of topic
	QueueSubscribersMap map[string]int      // topic : number of subscribers
	URLQueueMap         map[string]string   // topic:URL of the queue
	RwMtx               sync.RWMutex        // to guarantee access in mutual exclusion to the maps
	Zone                string
	TopicARN            string // sns arn notification endpoint
	QueueURL            string // sns queue reception
}

type RPCServer interface {
	MakeSubscription(inArg *utilities.RequestArg, outArg *utilities.SubscriptionOutput) error
	DeleteSubscription(inArg *utilities.RequestArg, exitStatus *int) error
	GenerateUserId(inArg *utilities.RequestArg, outId *string) error
	GetQueueURL(inArg *utilities.RequestArg, outURL *string) error
}

func (s *Service) GetQueueURL(inArg *utilities.RequestArg, outURL *string) error {
	s.RwMtx.RLock()
	/*start critical read section*/
	var l []string
	// check if the user is valid or not
	if val, exists := s.UsersIdMap[inArg.ID]; exists {
		l = val
	} else {
		s.RwMtx.RUnlock()
		return errors.New("invalid user id\n")
	}

	isSubscriber := false
	for i := 0; i < len(l); i++ {
		if l[i] == inArg.Tag {
			// the subscription already exists
			isSubscriber = true
			break
		}
	}
	if isSubscriber {
		*outURL = s.URLQueueMap[inArg.Tag]
		if *outURL == "" {
			var queueName string
			queueName = inArg.Tag + "_" + s.Zone
			// we haven't a valid reference to the queue
			result, err := sqsManagement.GetQueueURL(&queueName)
			if err != nil {
				fmt.Println("Got an error getting the queue URL:")
				fmt.Println(err)
			}
			if *result.QueueUrl == "" {
				//queue must be created
				*outURL = s.initQueue(inArg.Tag)
			} else {
				s.URLQueueMap[inArg.Tag] = *result.QueueUrl
				*outURL = *result.QueueUrl
			}
		}

	} else {
		s.RwMtx.RUnlock()
		return errors.New("a subscription must be done before")
	}

	s.RwMtx.RUnlock()
	return nil
}

func (s *Service) DeleteSubscription(inArg *utilities.RequestArg, exitStatus *int) error {
	s.RwMtx.Lock()
	/*critical section, nobody can read while i'm writing*/
	var l []string
	// check if the user is valid or not
	if val, exists := s.UsersIdMap[inArg.ID]; exists {
		l = val
	} else {
		s.RwMtx.Unlock()
		return errors.New("invalid user id\n")
	}

	//remove the element corresponding to the tag
	for i := 0; i < len(l); i++ {
		if l[i] == inArg.Tag {
			// the subscription exists
			s.UsersIdMap[inArg.ID] = append(l[:i], l[i+1:]...)
			s.QueueSubscribersMap[inArg.Tag]--
			break
		}
	}

	if s.QueueSubscribersMap[inArg.Tag] == 0 {
		//no more producers,  no more subscribers are still interested and so we can cancel this queue
		delete(s.QueueSubscribersMap, inArg.Tag)
		url := s.URLQueueMap[inArg.Tag]
		//deleting sqs-queue
		deleteQueue(&url)
		delete(s.URLQueueMap, inArg.Tag)
	}
	*exitStatus = 0
	s.RwMtx.Unlock()
	go func() { snsManagement.PublishUserListUpdate(s.UsersIdMap, &s.TopicARN) }()
	return nil
}

func (s *Service) MakeSubscriptionToTopic(inArg *utilities.RequestArg, outArg *utilities.SubscriptionOutput) error {
	s.RwMtx.Lock()
	/*critical section, nobody can read while i'm writing*/

	var l []string
	// check if the user is valid or not
	if val, exists := s.UsersIdMap[inArg.ID]; exists {
		l = val
	} else {
		s.RwMtx.Unlock()
		return errors.New("invalid user id\n")
	}

	for i := 0; i < len(l); i++ {
		if l[i] == inArg.Tag {
			s.RwMtx.Unlock()
			// the subscription already exists
			return errors.New("subscription already exists\n")
		}
	}
	//insert the tag into the list associated with the user
	s.UsersIdMap[inArg.ID] = append(l, inArg.Tag)

	if val, exists := s.URLQueueMap[inArg.Tag]; exists {
		outArg.QueueURL = val
		//increase the number of subscribers
		s.QueueSubscribersMap[inArg.Tag]++
	} else {
		//the queue doesn't exists
		//follows dynamic creation of the queue
		s.URLQueueMap[inArg.Tag] = s.initQueue(inArg.Tag + "_" + s.Zone)
		outArg.QueueURL = s.URLQueueMap[inArg.Tag]
		// this is a new queue with only a subscriber
		s.QueueSubscribersMap[inArg.Tag] = 1
	}
	s.RwMtx.Unlock()
	//need to send my list updated to other servers
	go func() { snsManagement.PublishUserListUpdate(s.UsersIdMap, &s.TopicARN) }()
	return nil
}

func (s *Service) GenerateUserId(inArgs *utilities.RequestArg, outId *string) error {
	s.RwMtx.Lock()
	/*critical section, nobody can read while i'm writing*/

	var ID string
	var alreadyExists bool
	for {
		ID = shortuuid.New()
		fmt.Printf("Generated ID: %s\n", ID)
		_, alreadyExists = s.UsersIdMap[ID]
		if !alreadyExists {
			break
		}
	}

	var l []string
	s.UsersIdMap[ID] = l
	*outId = ID
	s.RwMtx.Unlock()
	//need to send my list updated to other servers
	go func() { snsManagement.PublishUserListUpdate(s.UsersIdMap, &s.TopicARN) }()
	return nil
}

func (s *Service) initQueue(tag string) string {
	// Create a session that gets credential values from ~/.aws/credentials
	// and the default region from ~/.aws/config
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	result, err := sqsManagement.CreateQueue(sess, &tag)
	if err != nil {
		fmt.Println("Got an error creating the queue:")
		fmt.Println(err)
		return ""
	}

	return *result.QueueUrl

}

func deleteQueue(url *string) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	err := sqsManagement.DeleteQueue(sess, url)
	if err != nil {
		fmt.Println("You'll have to delete queue " + " yourself")
		fmt.Println(err)
		return
	}
}
