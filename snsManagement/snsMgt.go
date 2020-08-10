package snsManagement

import (
	"SDCC-A3-Project/sqsManagement"
	"SDCC-A3-Project/utilities"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"log"
	"strings"
)

// ShowTopics retrieves information about the Amazon SNS topics
func ShowTopics(svc snsiface.SNSAPI) (*sns.ListTopicsOutput, error) {
	results, err := svc.ListTopics(nil)
	return results, err
}

func findMAsterTopic() (topicARN *string) {
	var resTopicARN *string
	resTopicARN = nil
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sns.New(sess)

	results, err := ShowTopics(svc)
	if err != nil {
		fmt.Println("Got an error retrieving information about the SNS topics:")
		fmt.Println(err)
		return
	}

	for _, t := range results.Topics {
		if strings.HasSuffix(*t.TopicArn, "MASTER") {
			fmt.Println("found!!!")
			resTopicARN = t.TopicArn
		}
	}

	if resTopicARN == nil {
		//master topic doesn't exists
		topicName := "MASTER"
		result, err := MakeTopic(svc, &topicName)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		resTopicARN = result.TopicArn
	}

	return resTopicARN
}

func SnsToSqsConfig(outQueueURL, outTopicARN *string) {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Create new services for SQS and SNS
	sqsSvc := sqs.New(sess)
	snsSvc := sns.New(sess)

	topicName := "MASTER"
	queueRes, err := sqsManagement.CreateQueue(sess, &topicName)
	if err != nil {
		log.Fatal("Got an error creating the queue:", err)
	}

	protocolName := "sqs"
	topicArn := *findMAsterTopic()
	*outQueueURL = *queueRes.QueueUrl
	// No way to retrieve the queue ARN through the SDK, manual string replace to generate the ARN
	queueARN := convertQueueURLToARN(*queueRes.QueueUrl)

	subscribeQueueInput := sns.SubscribeInput{
		TopicArn: &topicArn,
		Protocol: &protocolName,
		Endpoint: &queueARN,
	}

	createSubRes, err := snsSvc.Subscribe(&subscribeQueueInput)

	if err != nil {
		fmt.Println(err.Error())
	}

	if createSubRes != nil {
		fmt.Println("connected with other servers using this link: " + *createSubRes.SubscriptionArn)
	}

	policyContent := "{\"Version\": \"2012-10-17\",  \"Id\": \"" + queueARN + "/SQSDefaultPolicy\",  \"Statement\": [    {     \"Sid\": \"Sid1580665629194\",      \"Effect\": \"Allow\",      \"Principal\": {        \"AWS\": \"*\"      },      \"Action\": \"SQS:SendMessage\",      \"Resource\": \"" + queueARN + "\",      \"Condition\": {        \"ArnEquals\": {         \"aws:SourceArn\": \"" + topicArn + "\"        }      }    }  ]}"

	attr := make(map[string]*string, 1)
	attr["Policy"] = &policyContent

	setQueueAttrInput := sqs.SetQueueAttributesInput{
		QueueUrl:   queueRes.QueueUrl,
		Attributes: attr,
	}

	_, err = sqsSvc.SetQueueAttributes(&setQueueAttrInput)

	if err != nil {
		fmt.Println(err.Error())
	}

	*outTopicARN = topicArn
}

func convertQueueURLToARN(inputURL string) string {
	// Awfully bad string replace code to convert a SQS queue URL to an ARN
	queueARN := strings.Replace(strings.Replace(strings.Replace(inputURL, "https://sqs.", "arn:aws:sqs:", -1), ".amazonaws.com/", ":", -1), "/", ":", -1)

	return queueARN
}

// MakeTopic creates an Amazon SNS topic
func MakeTopic(svc snsiface.SNSAPI, topic *string) (*sns.CreateTopicOutput, error) {
	results, err := svc.CreateTopic(&sns.CreateTopicInput{
		Name: topic,
	})
	return results, err
}

// PublishMessage publishes a message to an Amazon SNS topic
// Inputs:
//     svc is an Amazon SNS service object
//     msg is the message to publish
//     topicARN is the Amazon Resource Name (ARN) of the topic to publish through
// Output:
//     If success, information about the publication and nil
//     Otherwise, nil and an error from the call to Publish
func PublishMessage(svc snsiface.SNSAPI, msg, topicARN *string) (*sns.PublishOutput, error) {
	result, err := svc.Publish(&sns.PublishInput{
		Message:  msg,
		TopicArn: topicARN,
	})
	return result, err
}

func PublishUserListUpdate(userIdMap map[string][]string, topicARN *string) {
	msg := utilities.MapToJson(userIdMap)
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc := sns.New(sess)
	result, err := PublishMessage(svc, msg, topicARN)
	if err != nil {
		fmt.Println("Got an error publishing the message:")
		fmt.Println(err)
		return
	}

	fmt.Println("Message ID: " + *result.MessageId)

}
