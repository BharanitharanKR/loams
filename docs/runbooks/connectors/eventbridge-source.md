# Reading an EventBridge bus into Loams

The `eventbridge` source reads events that an EventBridge rule sends to an SQS queue (see [the guide](../../guides/connectors/eventbridge.md)). This runbook creates the queue, the rule and the permissions, then the instance. The commands use the AWS CLI. The same calls exist in CloudFormation and Terraform.

## 1. The queue

```sh
aws sqs create-queue --queue-name loams-eb-orders \
  --attributes VisibilityTimeout=60,MessageRetentionPeriod=1209600
QUEUE_URL=$(aws sqs get-queue-url --queue-name loams-eb-orders --query QueueUrl --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)
```

Add a dead-letter queue with a redrive policy (`maxReceiveCount` 5, for example). A message larger than the instance's batch budget is reported to the Loams DLQ and left for SQS's redrive to move.

## 2. The rule

```sh
aws events put-rule --name loams-orders --event-bus-name default \
  --event-pattern '{"source": ["com.example.shop"]}'
RULE_ARN=$(aws events describe-rule --name loams-orders --query Arn --output text)
```

## 3. Let the rule send to the queue

```sh
POLICY=$(jq -cn --arg q "$QUEUE_ARN" --arg r "$RULE_ARN" '{Version: "2012-10-17", Statement: [{
  Effect: "Allow", Principal: {Service: "events.amazonaws.com"}, Action: "sqs:SendMessage",
  Resource: $q, Condition: {ArnEquals: {"aws:SourceArn": $r}}}]}')
jq -n --arg p "$POLICY" '{Policy: $p}' > queue-attributes.json
aws sqs set-queue-attributes --queue-url "$QUEUE_URL" --attributes file://queue-attributes.json
aws events put-targets --rule loams-orders --event-bus-name default \
  --targets "Id=loams,Arn=$QUEUE_ARN"
```

Keep the target's input as the whole event, with no input transformer. The source reads `id`, `detail-type`, `time` and `detail` from it, and passes a CloudEvent in `detail` through.

## 4. The instance's credentials

Create an IAM principal for the instance with `sqs:ReceiveMessage`, `sqs:DeleteMessage` and `sqs:ChangeMessageVisibility` on the queue, and nothing else. Store its keys as Loams secrets, then name the secrets in the config. The config never holds the keys.

```yaml
config:
  region: eu-west-1
  queue_url: https://sqs.eu-west-1.amazonaws.com/123456789012/loams-eb-orders
  visibility_timeout_seconds: 60
  auth:
    access_key_id: eb-orders-key-id
    secret_access_key: eb-orders-secret
```

## 5. Check

Put a test event (`aws events put-events --entries '[{"Source":"com.example.shop","DetailType":"Ping","Detail":"{}"}]'`) and look for `io.loams.dev.flow.eventbridge.event.v1` in the instance's topic. If nothing arrives, check the rule's `FailedInvocations` metric in CloudWatch, which usually means the queue policy is missing, and the queue's `ApproximateNumberOfMessagesVisible`.
