package sqsManagement

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// CreateQueue creates an Amazon SQS queue
// Inputs:
//     sess is the
//     queueName is the name of the queue
// Output:
//     If success, the URL of the queue and nil
//     Otherwise, an empty string and an error from the call to CreateQueue
func CreateQueue(sess *session.Session, queue *string) (*sqs.CreateQueueOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)

	result, err := svc.CreateQueue(&sqs.CreateQueueInput{
		QueueName: queue,
		Attributes: map[string]*string{
			"DelaySeconds":           aws.String("60"),
			"MessageRetentionPeriod": aws.String("86400"),
		},
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

func DeleteQueue(sess *session.Session, queueURL *string) error {
	// Create an SQS service client
	svc := sqs.New(sess)

	_, err := svc.DeleteQueue(&sqs.DeleteQueueInput{
		QueueUrl: queueURL,
	})
	if err != nil {
		return err
	}

	return nil
}

// SendMsg sends a message to an Amazon SQS queue
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     queueURL is the URL of the queue
// Output:
//     If success, nil
//     Otherwise, an error from the call to SendMessage
func SendMsg(sess *session.Session, queueURL *string, message *string, author *string) error {
	// Create an SQS service client
	svc := sqs.New(sess)

	_, err := svc.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(10),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"Author": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(*author),
			},
		},
		MessageBody: aws.String(*message),
		QueueUrl:    queueURL,
	})
	if err != nil {
		return err
	}

	return nil
}

//GetMessages gets the messages from an Amazon SQS queue
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     queueURL is the URL of the queue
//     timeout is how long, in seconds, the message is unavailable to other consumers
// Output:
//     If success, the latest message and nil
//     Otherwise, nil and an error from the call to ReceiveMessage
func GetMessages(sess *session.Session, queueURL *string, timeout *int64) (*sqs.ReceiveMessageOutput, error) {
	// Create an SQS service client
	svc := sqs.New(sess)

	msgResult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   timeout,
	})

	if err != nil {
		return nil, err
	}

	return msgResult, nil
}

// DeleteMessage deletes a message from an Amazon SQS queue
// Inputs:
//     sess is the current session, which provides configuration for the SDK's service clients
//     queueURL is the URL of the queue
//     messageID is the ID of the message
// Output:
//     If success, nil
//     Otherwise, an error from the call to DeleteMessage
func DeleteMessage(sess *session.Session, queueURL *string, messageHandle *string) error {
	svc := sqs.New(sess)

	_, err := svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: messageHandle,
	})
	if err != nil {
		return err
	}
	return nil
}
