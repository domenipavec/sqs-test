package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func getQueue(sqsInstance *sqs.SQS, queueName string) (*string, error) {
	queueURLOutput, err := sqsInstance.GetQueueURL(&sqs.GetQueueURLInput{QueueName: &queueName})

	if err != nil {
		return nil, err
	}

	return queueURLOutput.QueueURL, nil
}

func createQueue(sqsInstance *sqs.SQS, queueName string) (*string, error) {
	createQueueOutput, err := sqsInstance.CreateQueue(&sqs.CreateQueueInput{QueueName: &queueName})

	if err != nil {
		return nil, err
	}

	return createQueueOutput.QueueURL, nil
}

func makeReceiveMessageInput(queueURL *string) *sqs.ReceiveMessageInput {
	var maxNumberOfMessages int64 = 1
	allString := "All"
	allArray := []*string{&allString}
	var visibilityTimeout int64 = 0
	var waitTimeSeconds int64 = 20
	return &sqs.ReceiveMessageInput{
		AttributeNames:        allArray,
		MaxNumberOfMessages:   &maxNumberOfMessages,
		MessageAttributeNames: allArray,
		QueueURL:              queueURL,
		VisibilityTimeout:     &visibilityTimeout,
		WaitTimeSeconds:       &waitTimeSeconds,
	}
}

func sendMessage(sqsInstance *sqs.SQS, queueURL *string, message string) error {
	_, err := sqsInstance.SendMessage(&sqs.SendMessageInput{
		QueueURL:    queueURL,
		MessageBody: &message,
	})
	return err
}

func printMessage(message *sqs.Message) {
	fmt.Println("attributes:")
	for k, v := range message.Attributes {
		fmt.Println(k, *v)
	}

	fmt.Println("body:")
	fmt.Println(*message.Body)

	fmt.Println("message attributes:")
	for k, _ := range message.MessageAttributes {
		fmt.Println(k)
	}

	if message.MessageID != nil {
		fmt.Println("message id:", *message.MessageID)
	} else {
		fmt.Println("message id: nil")
	}

	if message.ReceiptHandle != nil {
		fmt.Println("receipt handle:", *message.ReceiptHandle)
	} else {
		fmt.Println("receipt handle: nil")
	}
}

func main() {
	sqsInstance := sqs.New(&aws.Config{Region: "us-east-1"})

	queueURL, err := getQueue(sqsInstance, "domen-test-sqs")
	if err != nil {
		fmt.Println("GetQueue:", err)
		return
	}

	fmt.Println("QueueURL:", *queueURL)

	// send message after 10s
	go func() {
		time.Sleep(10 * time.Second)
		err := sendMessage(sqsInstance, queueURL, "test message")
		if err != nil {
			fmt.Println("SendMessage:", err)
		}
	}()

	// receive loop
	receiveMessageInput := makeReceiveMessageInput(queueURL)
	for {
		receiveMessageOutput, err := sqsInstance.ReceiveMessage(receiveMessageInput)
		if err != nil {
			fmt.Println("ReceiveMessage:", err)
			return
		}

		if len(receiveMessageOutput.Messages) < 1 {
			fmt.Println("no messages received")
			continue
		}

		message := receiveMessageOutput.Messages[0]
		fmt.Println("New message:")
		printMessage(message)
		
		// delete received message
		_, err = sqsInstance.DeleteMessage(&sqs.DeleteMessageInput{
			QueueURL: queueURL,
			ReceiptHandle: message.ReceiptHandle,
		})
		if err != nil {
			fmt.Println("DeleteMessage:", err)
		}
	}
}
