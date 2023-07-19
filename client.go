package sqsextendedclient

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
)

const (
	maxAllowedAttributes = 10 - 1 // 1 is reserved for the extended client attribute
	// RECEIPT_HANDLER_MATCHER = re.compile(r"^-\.\.s3BucketName\.\.-(.*)-\.\.s3BucketName\.\.--\.\.s3Key\.\.-(.*)-\.\.s3Key\.\.-(.*)")
	s3BucketNameMarker = "-..s3BucketName..-"
	s3KeyMarker        = "-..s3Key..-"
)

type Client struct {
	*sqs.Client
	s3c                  *s3.Client
	bucketName           string
	messageSizeThreshold int64
	alwaysThroughS3      bool
	pointerClass         string
	attributeName        string
}

type ClientOption func(*Client) error

func New(
	sqsc *sqs.Client,
	s3c *s3.Client,
	bucketName string,
	optFns ...ClientOption,
) (*Client, error) {
	c := Client{
		Client:               sqsc,
		s3c:                  s3c,
		bucketName:           bucketName,
		messageSizeThreshold: 262144, // 256 KiB
		pointerClass:         "software.amazon.payloadoffloading.PayloadS3Pointer",
		attributeName:        "ExtendedPayloadSize",
	}

	for _, optFn := range optFns {
		err := optFn(&c)
		if err != nil {
			return nil, err
		}
	}

	return &c, nil
}

func WithLogger() ClientOption {
	return func(c *Client) error {
		return nil
	}
}

func WithMessageSizeThreshold(size int) ClientOption {
	return func(c *Client) error {
		c.messageSizeThreshold = int64(size)
		return nil
	}
}

func WithAlwaysS3(alwaysS3 bool) ClientOption {
	return func(c *Client) error {
		c.alwaysThroughS3 = alwaysS3
		return nil
	}
}

// Override the default AttributeName with a custom value (i.e. "SQSLargePayloadSize")
func WithAttributeName(attributeName string) ClientOption {
	return func(c *Client) error {
		c.attributeName = attributeName
		return nil
	}
}

// Override default PointerClass with custom value (i.e.
// "com.amazon.sqs.javamessaging.MessageS3Pointer")
func WithPointerClass(pointerClass string) ClientOption {
	return func(c *Client) error {
		c.pointerClass = pointerClass
		return nil
	}
}

func (c *Client) messageExceedsThreshold(body *string, attributes map[string]types.MessageAttributeValue) bool {
	return int64(len(*body))+c.attributeSize(attributes) > c.messageSizeThreshold
}

func (c *Client) attributeSize(attributes map[string]types.MessageAttributeValue) int64 {
	sum := &atomic.Int64{}
	var wg sync.WaitGroup
	for k, v := range attributes {
		wg.Add(1)
		go func(k string, attr types.MessageAttributeValue) {
			sum.Add(int64(len([]byte(k))))
			sum.Add(int64(len(attr.BinaryValue)))
			sum.Add(int64(len(*attr.StringValue)))
			wg.Done()
		}(k, v)
	}
	wg.Wait()
	return sum.Load()
}

type s3Pointer struct {
	S3BucketName string
	S3Key        string
	class        string
}

func (p *s3Pointer) UnmarshalJSON(in []byte) error {
	// TODO: given the trivial structure, might just use Regex to parse out fields
	ptr := []interface{}{}

	if err := json.Unmarshal(in, &ptr); err != nil {
		return err
	}

	if len(ptr) != 2 {
		return fmt.Errorf("invalid pointer format, expected length 2, but received [%d]", len(ptr))
	}

	p.S3BucketName = ptr[1].(map[string]interface{})["s3BucketName"].(string)
	p.S3Key = ptr[1].(map[string]interface{})["s3Key"].(string)
	p.class = ptr[0].(string)

	return nil
}

func (p *s3Pointer) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`["%s",{"s3BucketName":"%s","s3Key":"%s"}]`, p.class, p.S3BucketName, p.S3Key)), nil
}

// Extended SQS Client wrapper around `sqs.SendMessage`. If the provided message exceeds the message
// size threshold (defaults to 256KiB), then the message will be uploaded to S3. Assuming a
// successful upload, the
//
// Delivers a message to the specified queue. A message can include only XML, JSON, and unformatted
// text. The following Unicode characters are allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to
// #xFFFD | #x10000 to #x10FFFF Any characters not included in this list will be rejected. For more
// information, see the W3C specification for characters (http://www.w3.org/TR/REC-xml/#charsets).
func (c *Client) SendMessage(
	ctx context.Context,
	params *sqs.SendMessageInput,
	optFns ...func(*sqs.Options),
) (*sqs.SendMessageOutput, error) {
	// copy to avoid mutating params
	input := *params

	if len(input.MessageAttributes) > maxAllowedAttributes {
		// TODO: update type to match AWS errors
		return nil, fmt.Errorf("number of message attributes [%d] exceeds the allowed maximum for large-payload messages [%d]", len(input.MessageAttributes), maxAllowedAttributes)
	}

	if c.alwaysThroughS3 || c.messageExceedsThreshold(input.MessageBody, input.MessageAttributes) {
		updatedAttributes := make(map[string]types.MessageAttributeValue, len(input.MessageAttributes)+1)
		for k, v := range input.MessageAttributes {
			updatedAttributes[k] = v
		}

		updatedAttributes[c.attributeName] = types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.Itoa(len(*input.MessageBody))),
		}

		input.MessageAttributes = updatedAttributes

		s3Key := uuid.New().String()
		_, err := c.s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &c.bucketName,
			Key:    aws.String(s3Key),
			Body:   strings.NewReader(*input.MessageBody),
		})

		if err != nil {
			return nil, fmt.Errorf("unable to upload large-payload to s3: %w", err)
		}

		asBytes, err := json.Marshal(&s3Pointer{
			S3BucketName: c.bucketName,
			S3Key:        s3Key,
			class:        c.pointerClass,
		})

		if err != nil {
			return nil, fmt.Errorf("unable to upload large-payload to s3: %w", err)
		}

		input.MessageBody = aws.String(string(asBytes))
	}

	return c.Client.SendMessage(ctx, &input, optFns...)
}

// You can use SendMessageBatch to send up to 10 messages to the specified queue by assigning either
// identical or different values to each message (or by not assigning values at all). This is a
// batch version of SendMessage . For a FIFO queue, multiple messages within a single batch are
// enqueued in the order they are sent. The result of sending each message is reported individually
// in the response. Because the batch request can result in a combination of successful and
// unsuccessful actions, you should check for batch errors even when the call returns an HTTP status
// code of 200 . The maximum allowed individual message size and the maximum total payload size (the
// sum of the individual lengths of all of the batched messages) are both 256 KiB (262,144 bytes). A
// message can include only XML, JSON, and unformatted text. The following Unicode characters are
// allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF Any characters
// not included in this list will be rejected. For more information, see the W3C specification for
// characters (http://www.w3.org/TR/REC-xml/#charsets) . If you don't specify the DelaySeconds
// parameter for an entry, Amazon SQS uses the default value for the queue.
func (c *Client) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	return c.Client.SendMessageBatch(ctx, params, optFns...)
}

// Retrieves one or more messages (up to 10), from the specified queue. Using the WaitTimeSeconds
// parameter enables long-poll support. For more information, see Amazon SQS Long Polling
// (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html)
// in the Amazon SQS Developer Guide. Short poll is the default behavior where a weighted random set
// of machines is sampled on a ReceiveMessage call. Thus, only the messages on the sampled machines
// are returned. If the number of messages in the queue is small (fewer than 1,000), you most likely
// get fewer messages than you requested per ReceiveMessage call. If the number of messages in the
// queue is extremely small, you might not receive any messages in a particular ReceiveMessage
// response. If this happens, repeat the request. For each message returned, the response includes
// the following:
//   - The message body.
//   - An MD5 digest of the message body. For information about MD5, see RFC1321 (https://www.ietf.org/rfc/rfc1321.txt)
//     .
//   - The MessageId you received when you sent the message to the queue.
//   - The receipt handle.
//   - The message attributes.
//   - An MD5 digest of the message attributes.
//
// The receipt handle is the identifier you must provide when deleting the message. For more
// information, see Queue and Message Identifiers
// (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-queue-message-identifiers.html)
// in the Amazon SQS Developer Guide. You can provide the VisibilityTimeout parameter in your
// request. The parameter is applied to the messages that Amazon SQS returns in the response. If you
// don't include the parameter, the overall visibility timeout for the queue is used for the
// returned messages. For more information, see Visibility Timeout
// (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
// in the Amazon SQS Developer Guide. A message that isn't deleted or a message whose visibility
// isn't extended before the visibility timeout expires counts as a failed receive. Depending on the
// configuration of the queue, the message might be sent to the dead-letter queue. In the future,
// new attributes might be added. If you write code that calls this action, we recommend that you
// structure your code so that it can handle new attributes gracefully.
func (c *Client) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	// if MessageAttributeNames doesn't include RESERVED_ATTRIBUTE_NAME and not 'All' in MessageAttributeNames or '.*' in MessageAttributeNames
	// 		add RESERVED_ATTRIBUTE_NAME to MessageAttributeNames
	// call underlying RetrieveMessage
	// for each message in response
	// 		extract MessageAttributes, Body, ReceiptHandle
	// 		ensure RESERVED_ATTRIBUTE_NAME in MessageAttributes
	// 		ensure Body is List, of length 2, and item 0 is equal to MESSAGE_POINTER_CLASS
	// 		parse element[1] and extract the s3BucketName and s3Key
	// 		extract message from S3
	// 		update receipt_handle to be:
	// 			{S3_BUCKET_NAME_MARKER}{bucket}{S3_BUCKET_NAME_MARKER}{S3_KEY_MARKER}{key}{S3_KEY_MARKER}{receipt_handle}
	// 		update message body with one from S3
	// 		update message attributes to be all except for RESERVED_ATTRIBUTE_NAME
	return c.Client.ReceiveMessage(ctx, params, optFns...)
}

// Deletes the specified message from the specified queue. To select the message to delete, use the
// ReceiptHandle of the message (not the MessageId which you receive when you send the message).
// Amazon SQS can delete a message from a queue even if a visibility timeout setting causes the
// message to be locked by another consumer. Amazon SQS automatically deletes messages left in a
// queue longer than the retention period configured for the queue. The ReceiptHandle is associated
// with a specific instance of receiving a message. If you receive a message more than once, the
// ReceiptHandle is different each time you receive a message. When you use the DeleteMessage
// action, you must provide the most recently received ReceiptHandle for the message (otherwise, the
// request succeeds, but the message will not be deleted). For standard queues, it is possible to
// receive a message even after you delete it. This might happen on rare occasions if one of the
// servers which stores a copy of the message is unavailable when you send the request to delete the
// message. The copy remains on the server and might be returned to you during a subsequent receive
// request. You should ensure that your application is idempotent, so that receiving a message more
// than once does not cause issues.
func (c *Client) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	// if the RECEIPT_HANDLER_MATCHER regex matches the ReceiptHandle
	// s3.delete_objects for the provided S3 bucket and path
	// call source fn
	return c.Client.DeleteMessage(ctx, params, optFns...)
}

func (c *Client) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	// if the RECEIPT_HANDLER_MATCHER regex matches the ReceiptHandle
	// s3.delete_objects for the provided S3 bucket and path
	// call source fn
	return c.Client.DeleteMessageBatch(ctx, params, optFns...)
}
