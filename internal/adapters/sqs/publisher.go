package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/patrickkaua/wagering/internal/app"
	"github.com/patrickkaua/wagering/internal/domain"
)

var _ app.EventPublisher = (*Publisher)(nil)

type sqsSender interface {
	SendMessage(ctx context.Context, params *awssqs.SendMessageInput, optFns ...func(*awssqs.Options)) (*awssqs.SendMessageOutput, error)
}

// Publisher publishes domain events to an SQS FIFO queue.
type Publisher struct {
	client   sqsSender
	queueURL string
}

// NewPublisher creates an event publisher for the specified queue URL.
func NewPublisher(client *awssqs.Client, queueURL string) *Publisher {
	return &Publisher{
		client:   client,
		queueURL: queueURL,
	}
}

// Publish serializes and sends a domain event to the SQS FIFO events queue.
func (p *Publisher) Publish(ctx context.Context, e domain.Event) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = p.client.SendMessage(ctx, &awssqs.SendMessageInput{
		QueueUrl:               aws.String(p.queueURL),
		MessageBody:            aws.String(string(body)),
		MessageGroupId:         aws.String(e.AggregateID.String()),
		MessageDeduplicationId: aws.String(e.EventID.String()),
		MessageAttributes: map[string]types.MessageAttributeValue{
			"eventType": {
				DataType:    aws.String("String"),
				StringValue: aws.String(e.EventType),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("publish event to sqs: %w", err)
	}

	return nil
}
