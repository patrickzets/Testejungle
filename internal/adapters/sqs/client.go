package sqs

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/patrickzets/Testejungle/internal/platform"
)

// NewClient creates an AWS SQS client configured from platform.Config.
func NewClient(ctx context.Context, cfg platform.Config) (*awssqs.Client, error) {
	optFns := []func(*config.LoadOptions) error{
		config.WithRegion(cfg.AWSRegion),
	}

	if cfg.AWSEndpointURL != "" {
		optFns = append(optFns, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	client := awssqs.NewFromConfig(awsCfg, func(o *awssqs.Options) {
		if cfg.AWSEndpointURL != "" {
			o.BaseEndpoint = aws.String(cfg.AWSEndpointURL)
		}
	})

	return client, nil
}

// QueueHealth checks connectivity to SQS queues by querying their attributes.
func QueueHealth(client *awssqs.Client, queueURLs ...string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for _, url := range queueURLs {
			if url == "" {
				continue
			}
			_, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{
				QueueUrl: aws.String(url),
				AttributeNames: []types.QueueAttributeName{
					types.QueueAttributeNameQueueArn,
				},
			})
			if err != nil {
				return fmt.Errorf("sqs health check failed for %s: %w", url, err)
			}
		}
		return nil
	}
}
