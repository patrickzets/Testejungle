#!/usr/bin/env bash
set -euo pipefail

echo "Initializing LocalStack SQS queues..."

# 1. Create Dead Letter Queue (DLQ)
awslocal sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

DLQ_URL=$(awslocal sqs get-queue-url --queue-name wager-transactions-dlq.fifo --query 'QueueUrl' --output text)
DLQ_ARN=$(awslocal sqs get-queue-attributes --queue-url "${DLQ_URL}" --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

echo "Created DLQ: ${DLQ_URL} with ARN: ${DLQ_ARN}"

# 2. Create inbound queue with RedrivePolicy and VisibilityTimeout=30 using json config
python3 -c "
import json
attrs = {
    'FifoQueue': 'true',
    'ContentBasedDeduplication': 'false',
    'DeduplicationScope': 'messageGroup',
    'FifoThroughputLimit': 'perMessageGroupId',
    'VisibilityTimeout': '30',
    'RedrivePolicy': json.dumps({'deadLetterTargetArn': '${DLQ_ARN}', 'maxReceiveCount': '5'})
}
with open('/tmp/wager-tx-attrs.json', 'w') as f:
    json.dump(attrs, f)
"

awslocal sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes file:///tmp/wager-tx-attrs.json

echo "Created wager-transactions.fifo with redrive to DLQ"

# 3. Create outbound events queue
awslocal sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=false

echo "Created wager-events.fifo"
echo "LocalStack SQS initialization complete."
