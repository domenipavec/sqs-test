package main

import (
	"fmt"

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

func main() {
	sqsInstance := sqs.New(&aws.Config{Region: "us-east-1"})

	queueURL, err := createQueue(sqsInstance, "domen-test-sqs")
	if err != nil {
        fmt.Println(err)
        return
    }
    
	fmt.Println(*queueURL)

	var maxNumberOfMessages int64 = 1
	allString := "All"
	allArray := []*string{&allString}
    var visibilityTimeout int64 = 0
    var waitTimeSeconds int64 = 20
	receiveMessageInput := &sqs.ReceiveMessageInput{
		AttributeNames:        allArray,
		MaxNumberOfMessages:   &maxNumberOfMessages,
		MessageAttributeNames: allArray,
        QueueURL: queueURL,
        VisibilityTimeout: &visibilityTimeout,
        WaitTimeSeconds: &waitTimeSeconds,
	}

	for {
		receiveMessageOutput, err := sqsInstance.ReceiveMessage(receiveMessageInput)
		if err != nil {
			fmt.Println(err)
			return
		}

        if len(receiveMessageOutput.Messages) < 1 {
            fmt.Println("no messages received")
        }
        
        message := receiveMessageOutput.Messages[0]
        fmt.Println("New message:")
        
        fmt.Println("attributes:")
        for k,v := range message.Attributes {
            fmt.Println(k,*v)
        }
        
        fmt.Println("body:")
        fmt.Println(*message.Body)
        
        fmt.Println("message attributes:")
        for k,_ := range message.MessageAttributes {
            fmt.Println(k)
        }
        
        fmt.Println("message id:", *message.MessageID)
        fmt.Println("receipt handle:", *message.ReceiptHandle)
    }
}
