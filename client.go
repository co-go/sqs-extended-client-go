package sqsextendedclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const (
	maxAllowedAttributes        = 10 - 1 // 1 is reserved for the extended client reserved attribute
	LegacyReservedAttributeName = "SQSLargePayloadSize"
	LegacyS3PointerClass        = "com.amazon.sqs.javamessaging.MessageS3Pointer"
	maxMsgSizeInBytes           = 1048576 // 1 MiB
)

var (
	jsonUnmarshal        = json.Unmarshal
	jsonMarshal          = json.Marshal
	validObjectNameRegex = regexp.MustCompile("^[0-9a-zA-Z!_.*'()-]+$")
	ErrObjectPrefix      = errors.New("object prefix contains invalid characters")
)

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

type Logger interface {
	Warn(msg string, args ...any)
}

// Client is a wrapper for the [github.com/aws/aws-sdk-go-v2/service/sqs.Client], providing extra
// functionality for retrieving, sending and deleting messages.
type Client struct {
	SQSClient
	s3c                       S3Client
	logger                    Logger
	bucketName                string
	messageSizeThreshold      int64
	batchMessageSizeThreshold int64
	alwaysThroughS3           bool
	skipDeleteS3Payloads      bool
	pointerClass              string
	reservedAttrs             []string
	objectPrefix              string
	baseS3PointerSize         int
	baseAttributeSize         int
}

type ClientOption func(*Client) error

// New returns a newly created [*Client] with defaults:
//   - MessageSizeThreshold: 1048576 (1 MiB)
//   - BatchMessageSizeThreshold: 1048576 (1 MiB)
//   - S3PointerClass: "software.amazon.payloadoffloading.PayloadS3Pointer"
//   - ReservedAttributeName: "ExtendedPayloadSize"
//
// Further options can be passed in to configure these or other options. See [ClientOption]
// functions for more details.
func New(
	sqsc SQSClient,
	s3c S3Client,
	optFns ...ClientOption,
) (*Client, error) {
	c := Client{
		SQSClient:                 sqsc,
		s3c:                       s3c,
		logger:                    slog.New(slog.NewTextHandler(os.Stdout, nil)),
		messageSizeThreshold:      maxMsgSizeInBytes,
		batchMessageSizeThreshold: maxMsgSizeInBytes,
		pointerClass:              "software.amazon.payloadoffloading.PayloadS3Pointer",
		reservedAttrs:             []string{"ExtendedPayloadSize", LegacyReservedAttributeName},
	}

	// apply optFns to the base client
	for _, optFn := range optFns {
		err := optFn(&c)
		if err != nil {
			return nil, err
		}
	}

	// create an example s3 pointer
	ptr := &s3Pointer{
		S3Key: uuid.NewString(),
		class: c.pointerClass,
	}

	// get its string representation
	s3PointerBytes, _ := ptr.MarshalJSON()

	// store the length of this string to be used when calculating optimal payload sizing in the
	// BatchSendMessage method. Note the size with the S3 Bucket is excluded here as this can
	// change on a per-call basis.
	c.baseS3PointerSize = len(s3PointerBytes)

	// similarly, store the base size of the attribute added for extended payloads. Note the string
	// representation of the length of the payload is also omitted here as that obviously changes
	// between each message.
	c.baseAttributeSize = len(c.reservedAttrs[0]) + len("Number")

	return &c, nil
}

// WithLogger allows the caller to control how messages will be logged from the client. The expected
// interface matches the `log/slog` function signature and will default to a TextHandler unless
// overwritten by this method.
func WithLogger(logger Logger) ClientOption {
	return func(c *Client) error {
		c.logger = logger
		return nil
	}
}

// Set the destination bucket for large messages that are sent by this client. This is a
// soft-requirement for using the SendMessage function.
func WithS3BucketName(bucketName string) ClientOption {
	return func(c *Client) error {
		c.bucketName = bucketName
		return nil
	}
}

// Set the MessageSizeThreshold to some other value (in bytes). By default this is 1048576 (1 MiB).
func WithMessageSizeThreshold(size int) ClientOption {
	return func(c *Client) error {
		c.messageSizeThreshold = int64(size)
		return nil
	}
}

// Set the BatchMessageSizeThreshold to some other value (in bytes). By default this is 1048576 (1
// MiB).
func WithBatchMessageSizeThreshold(size int) ClientOption {
	return func(c *Client) error {
		c.batchMessageSizeThreshold = int64(size)
		return nil
	}
}

// Set the behavior of the client to always send messages to S3, regardless of the size of their
// body or attributes. By default this is false.
func WithAlwaysS3(alwaysS3 bool) ClientOption {
	return func(c *Client) error {
		c.alwaysThroughS3 = alwaysS3
		return nil
	}
}

// WithSkipDeleteS3Payloads disables deletion of S3 payloads when deleting SQS messages. Defaults to
// false (meaning S3 payloads will be deleted). If set to true, S3 objects are left intact and
// should be managed via lifecycle policies or external cleanup.
func WithSkipDeleteS3Payloads(skip bool) ClientOption {
	return func(c *Client) error {
		c.skipDeleteS3Payloads = skip
		return nil
	}
}

// WithReservedAttributeNames allows the user of the client to provide a list of attributes that
// will be used to identify large messages both sent and received by the created client. When
// sending messages, only the first attribute provided will be attached to the MessageAttributes.
// When receiving messages, all provided attributes will be checked to determine if the message has
// an extended payload in S3.
func WithReservedAttributeNames(attributeNames []string) ClientOption {
	return func(c *Client) error {
		c.reservedAttrs = attributeNames
		return nil
	}
}

// Override PointerClass with custom value (i.e. [LegacyS3PointerClass])
func WithPointerClass(pointerClass string) ClientOption {
	return func(c *Client) error {
		c.pointerClass = pointerClass
		return nil
	}
}

// WithObjectPrefix attaches a prefix to the object key (prefix/uuid)
func WithObjectPrefix(prefix string) ClientOption {
	return func(c *Client) error {
		if !validObjectNameRegex.MatchString(prefix) {
			return ErrObjectPrefix
		}
		c.objectPrefix = prefix
		return nil
	}
}

// s3Key returns a new string object key and prepends c.ObjectPrefix if it exists.
func (c *Client) s3Key(filename string) string {
	if c.objectPrefix != "" {
		return fmt.Sprintf("%s/%s", c.objectPrefix, filename)
	}
	return filename
}

// messageSize describes the size of a SQS message (body and its attributes)
type messageSize struct {
	bodySize      int64
	attributeSize int64
}

// Total returns the full message size
func (m messageSize) Total() int64 {
	return m.bodySize + m.attributeSize
}

// ToExtendedSize will convert a messageSize to its equivalent extended payload size. This can be
// useful for estimating the size of a message if it were to be converted without actually having to
// handle the conversion.
func (m messageSize) ToExtendedSize(pointerSize, attributeSize int) messageSize {
	n, numDigits := int64(10), int64(1)
	for n <= m.bodySize {
		n *= 10
		numDigits++
	}

	return messageSize{
		bodySize:      int64(pointerSize),
		attributeSize: int64(attributeSize) + numDigits + m.attributeSize,
	}
}

// getMessageSize returns the size of the body and attributes of a message
func (c *Client) messageSize(body *string, attributes map[string]types.MessageAttributeValue) messageSize {
	return messageSize{
		bodySize:      int64(len(*body)),
		attributeSize: c.attributeSize(attributes),
	}
}

// messageExceedsThreshold determines if the size of the body and attributes exceeds the configured
// message size threshold
func (c *Client) messageExceedsThreshold(body *string, attributes map[string]types.MessageAttributeValue) bool {
	return c.messageSize(body, attributes).Total() > c.messageSizeThreshold
}

// attributeSize will return the size of all provided attributes and their values
func (c *Client) attributeSize(attributes map[string]types.MessageAttributeValue) int64 {
	sum := &atomic.Int64{}
	var wg sync.WaitGroup
	for k, v := range attributes {
		wg.Add(1)
		go func(k string, attr types.MessageAttributeValue) {
			sum.Add(int64(len([]byte(k))))
			sum.Add(int64(len(attr.BinaryValue)))

			if attr.StringValue != nil {
				sum.Add(int64(len(*attr.StringValue)))
			}

			if attr.DataType != nil {
				sum.Add(int64(len(*attr.DataType)))
			}

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
	ptr := []interface{}{}

	if err := jsonUnmarshal(in, &ptr); err != nil {
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

// Extended SQS Client wrapper around [github.com/aws/aws-sdk-go-v2/service/sqs.Client.SendMessage].
// If the provided message exceeds the message size threshold (defaults to 1 MiB), then the message
// will be uploaded to S3. Assuming a successful upload, the message will be altered by:
//
//  1. Adding a custom attribute under the configured reserved attribute name that contains the size
//     of the large payload.
//  2. Body of the original message overridden with a S3 Pointer to the newly created S3 location
//     that holds the entirety of the message
//
// The S3 bucket used for large messages can be specified at either the client level (through the
// WithS3BucketName [ClientOption]) or for an individual call by appending the QueueURL with a "|"
// and the bucket name. For example: "https://sqs.amazonaws.com/1234/queue|bucket-for-messages". If
// the bucket name is provided like this, it will override any S3 bucket that was provided at the
// client level.
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.SendMessage] for completeness:
//
// Delivers a message to the specified queue. A message can include only XML, JSON, and unformatted
// text. The following Unicode characters are allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to
// #xFFFD | #x10000 to #x10FFFF Any characters not included in this list will be rejected. For more
// information, see the [W3C] specification for characters.
//
// [W3C]: http://www.w3.org/TR/REC-xml/#charsets
func (c *Client) SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error) {
	// copy to avoid mutating params
	input := *params

	// determine bucket name, either from client (default) or from provided SQS URL
	queueURL, s3Bucket, found := strings.Cut(*params.QueueUrl, "|")
	if !found {
		s3Bucket = c.bucketName
	}

	input.QueueUrl = &queueURL

	if c.alwaysThroughS3 || c.messageExceedsThreshold(input.MessageBody, input.MessageAttributes) {
		// generate s3 object key
		s3Key := c.s3Key(uuid.NewString())

		// upload large payload to S3
		_, err := c.s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &s3Bucket,
			Key:    aws.String(s3Key),
			Body:   strings.NewReader(*input.MessageBody),
		})

		if err != nil {
			return nil, fmt.Errorf("unable to upload large payload to s3: %w", err)
		}

		// create an s3 pointer that will be uploaded to SQS in place of the large payload
		asBytes, err := jsonMarshal(&s3Pointer{
			S3BucketName: s3Bucket,
			S3Key:        s3Key,
			class:        c.pointerClass,
		})

		if err != nil {
			return nil, fmt.Errorf("unable to marshal S3 pointer: %w", err)
		}

		// copy over all attributes, leaving space for our reserved attribute
		updatedAttributes := make(map[string]types.MessageAttributeValue, len(input.MessageAttributes)+1)
		for k, v := range input.MessageAttributes {
			updatedAttributes[k] = v
		}

		// assign the reserved attribute to a number containing the size of the original body
		updatedAttributes[c.reservedAttrs[0]] = types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.Itoa(len(*input.MessageBody))),
		}

		// override attributes and body in the original message
		input.MessageAttributes = updatedAttributes
		input.MessageBody = aws.String(string(asBytes))
	}

	return c.SQSClient.SendMessage(ctx, &input, optFns...)
}

// batchMessageMeta is used to maintain a reference to the original payload inside of a batch
// request while also storing metadata about its size to be used during the optimize step
type batchMessageMeta struct {
	payloadIndex int
	msgSize      messageSize
}

// batchPayload stores information about a combination of messages in order to determine the most
// efficient batches
type batchPayload struct {
	batchBytes       int64
	s3PointerSize    int
	extendedMessages []batchMessageMeta
}

// optimizeBatchPayload will attempt to recursively determine the best way to distribute the passed
// in messages into extended and regular messages in order to optimize the resulting batchPayload
// across a series of factors. The most important factors for determining the best batch are:
//  1. Always prefer a payload that is under the batchMessageSizeThreshold
//  2. Prefer a payload with the LEAST amount of extended messages
//  3. Prefer a payload which sends the LEAST amount of data to S3
//
// These decisions are made in an effort to be cognizant about performance and costs of the caller.
func (c *Client) optimizeBatchPayload(bp *batchPayload, messages []batchMessageMeta) *batchPayload {
	// return if we have no more messages to examine
	if len(messages) == 0 {
		return bp
	}

	currMsg := messages[0]
	numExtMsg := len(bp.extendedMessages)

	// case 1 - assume we leave the message as-is
	c1 := c.optimizeBatchPayload(&batchPayload{
		batchBytes:       bp.batchBytes + currMsg.msgSize.Total(),
		extendedMessages: bp.extendedMessages,
		s3PointerSize:    bp.s3PointerSize,
	}, messages[1:])

	// case 2 - assume we convert the message into an extended payload
	extendedMessageSize := currMsg.msgSize.ToExtendedSize(bp.s3PointerSize, c.baseAttributeSize).Total()
	c2 := c.optimizeBatchPayload(&batchPayload{
		batchBytes:       bp.batchBytes + extendedMessageSize,
		extendedMessages: append(bp.extendedMessages[:numExtMsg:numExtMsg], currMsg),
		s3PointerSize:    bp.s3PointerSize,
	}, messages[1:])

	// preform the checks against factors provided in the function description
	if c1.batchBytes <= c.batchMessageSizeThreshold && c2.batchBytes > c.batchMessageSizeThreshold {
		return c1
	} else if c2.batchBytes <= c.batchMessageSizeThreshold && c1.batchBytes > c.batchMessageSizeThreshold {
		return c2
	} else if c1.batchBytes > c.batchMessageSizeThreshold && c2.batchBytes > c.batchMessageSizeThreshold {
		// in this case, both payloads suck- attempt to return the best of the worst
		if c1.batchBytes <= c2.batchBytes {
			return c1
		}
		return c2
	} else if len(c2.extendedMessages) > len(c1.extendedMessages) {
		return c1
	} else if c1.batchBytes > c2.batchBytes {
		return c1
	}

	return c2
}

// Extended SQS Client wrapper around
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.SendMessageBatch]. When preparing the messages
// for transport, if the size of any message exceeds the messageSizeThreshold or if alwaysS3 is set
// to true, the message will be uploaded to S3. For the remaining messages, this method will
// calculate the least amount of messages required to upload to S3 in order to reduce the overall
// payload size under the batchMessageSizeThreshold. If there are multiple combinations to reduce
// the payload below the threshold with uploading the same amount of messages, preference will be
// given to the combination that results in the smallest amount of data sent to S3 in order to
// minimize costs.
//
// For each message that is successfully uploaded to S3, the messages will be altered by:
//
//  1. Adding a custom attribute under the configured reserved attribute name that contains the size
//     of the large payload.
//  2. Body of the original message overridden with a S3 Pointer to the newly created S3 location
//     that holds the entirety of the message.
//
// After all applicable messages are uploaded to S3, then the SQS native SendMessageBatch call is
// invoked.
//
// The S3 bucket used for large messages can be specified at either the client level (through the
// WithS3BucketName [ClientOption]) or for an individual call by appending the QueueURL with a "|"
// and the bucket name. For example: "https://sqs.amazonaws.com/1234/queue|bucket-for-messages". If
// the bucket name is provided like this, it will override any S3 bucket that was provided at the
// client level.
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.SendMessageBatch] for completeness:
//
// You can use SendMessageBatch to send up to 10 messages to the specified queue by assigning either
// identical or different values to each message (or by not assigning values at all). This is a
// batch version of SendMessage. For a FIFO queue, multiple messages within a single batch are
// enqueued in the order they are sent. The result of sending each message is reported individually
// in the response. Because the batch request can result in a combination of successful and
// unsuccessful actions, you should check for batch errors even when the call returns an HTTP status
// code of 200 . The maximum allowed individual message size and the maximum total payload size (the
// sum of the individual lengths of all of the batched messages) are both 1 MiB (1,048,576 bytes). A
// message can include only XML, JSON, and unformatted text. The following Unicode characters are
// allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF Any characters
// not included in this list will be rejected. For more information, see the W3C specification for
// characters (http://www.w3.org/TR/REC-xml/#charsets). If you don't specify the DelaySeconds
// parameter for an entry, Amazon SQS uses the default value for the queue.
func (c *Client) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	input := *params
	copyEntries := make([]types.SendMessageBatchRequestEntry, len(input.Entries))

	// determine bucket name, either from client (default) or from provided SQS URL
	queueURL, s3Bucket, found := strings.Cut(*params.QueueUrl, "|")
	if !found {
		s3Bucket = c.bucketName
	}

	input.QueueUrl = &queueURL

	// initialize the payload struct which will hold the data describing the batch
	bp := &batchPayload{
		s3PointerSize:    c.baseS3PointerSize + len(s3Bucket),
		extendedMessages: make([]batchMessageMeta, 0, len(input.Entries)),
	}

	// store "regular" (non-extended) messages separately to iterate during optimize step
	regularMessages := make([]batchMessageMeta, 0, len(input.Entries))
	for i, e := range input.Entries {
		i, e := i, e

		// always copy the entry
		copyEntries[i] = e

		// calculate the starting message size
		msgSize := c.messageSize(e.MessageBody, e.MessageAttributes)

		// build a "meta" struct in order to keep track of this message when optimizing the batch
		// payload in the next stage
		msgMeta := batchMessageMeta{
			payloadIndex: i,
			msgSize:      msgSize,
		}

		// check if we always send through s3, or if the message size exceeds the threshold
		if c.alwaysThroughS3 || msgSize.Total() > c.messageSizeThreshold {
			// track the payload under the batch's extendedMessages
			bp.extendedMessages = append(bp.extendedMessages, msgMeta)

			// update the base payload size
			bp.batchBytes += msgSize.ToExtendedSize(bp.s3PointerSize, c.baseAttributeSize).Total()
		} else {
			regularMessages = append(regularMessages, msgMeta)
		}
	}

	// attempt to find the most efficient message combination for our batch
	bp = c.optimizeBatchPayload(bp, regularMessages)

	if bp.batchBytes > c.batchMessageSizeThreshold {
		c.logger.Warn(fmt.Sprintf("SendMessageBatch is only able to reduce the batch size to <%d> even though BatchMessageSizeThreshold is set to <%d>. Errors might occur.", bp.batchBytes, c.batchMessageSizeThreshold))
	}

	g := new(errgroup.Group)
	for _, em := range bp.extendedMessages {
		// generate s3 object key
		s3Key := c.s3Key(uuid.NewString())

		// deep copy full message payload to send to S3
		msgBody := *copyEntries[em.payloadIndex].MessageBody

		// upload large payload to S3
		g.Go(func() error {
			_, err := c.s3c.PutObject(ctx, &s3.PutObjectInput{
				Bucket: &s3Bucket,
				Key:    aws.String(s3Key),
				Body:   strings.NewReader(msgBody),
			})

			if err != nil {
				return fmt.Errorf("unable to upload large payload to s3: %w", err)
			}

			return nil
		})

		// create an s3 pointer that will be uploaded to SQS in place of the large payload
		asBytes, err := jsonMarshal(&s3Pointer{
			S3BucketName: s3Bucket,
			S3Key:        s3Key,
			class:        c.pointerClass,
		})

		if err != nil {
			return nil, fmt.Errorf("unable to marshal S3 pointer: %w", err)
		}

		// copy over all attributes, leaving space for our reserved attribute
		updatedAttributes := make(map[string]types.MessageAttributeValue, len(copyEntries[em.payloadIndex].MessageAttributes)+1)
		for k, v := range copyEntries[em.payloadIndex].MessageAttributes {
			updatedAttributes[k] = v
		}

		// assign the reserved attribute to a number containing the size of the original body
		updatedAttributes[c.reservedAttrs[0]] = types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.FormatInt(em.msgSize.bodySize, 10)),
		}

		// override attributes and body in the original message
		copyEntries[em.payloadIndex].MessageAttributes = updatedAttributes
		copyEntries[em.payloadIndex].MessageBody = aws.String(string(asBytes))
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// override entries with our copied ones
	input.Entries = copyEntries

	return c.SQSClient.SendMessageBatch(ctx, &input, optFns...)
}

// ReceiveMessage is a wrapper for the
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.ReceiveMessage] function, but it automatically
// retrieves S3 files for each applicable message returned by the internal ReceiveMessage call.
//
// For each record in the provided event, if the configured Reserved Attribute Name
// ("ExtendedPayloadSize" by default) IS NOT present, the record is copied over without change to
// the returned event. However, if the Reserved Attribute Name IS present, the body of the record
// will be parsed to determine the S3 location of the full message body. This S3 location is read,
// and the body of the record will be overwritten with the contents. The last update is made to the
// record's ReceiptHandle, setting it to a unique pattern for the Extended SQS Client to be able to
// delete the S3 file when the SQS message is deleted (see [*Client.DeleteMessage] for more
// details).
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.ReceiveMessage] for completeness:
//
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
//   - The receipt handle
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
	input := *params
	includesAttributeName := false
	includesAll := false

	// copy attributes over to avoid mutating
	messageAttributeCopy := make([]string, len(input.MessageAttributeNames)+len(c.reservedAttrs))
	for i, a := range input.MessageAttributeNames {
		messageAttributeCopy[i] = a
		if a == "All" || a == ".*" {
			includesAll = true
		}

		for _, reservedAttr := range c.reservedAttrs {
			if a == reservedAttr {
				includesAttributeName = true
			}
		}
	}

	// if the reserved attribute name is not present, add it to the list
	if !includesAttributeName && !includesAll {
		for i, reservedAttr := range c.reservedAttrs {
			messageAttributeCopy[len(input.MessageAttributeNames)+i] = reservedAttr
		}

		input.MessageAttributeNames = messageAttributeCopy
	}

	// call underlying SQS ReceiveMessage
	sqsResp, err := c.SQSClient.ReceiveMessage(ctx, &input, optFns...)

	if err != nil {
		return nil, err
	}

	g := new(errgroup.Group)

	for i, m := range sqsResp.Messages {
		i, m := i, m

		g.Go(func() error {
			found := false
			for _, reservedAttr := range c.reservedAttrs {
				if _, ok := m.MessageAttributes[reservedAttr]; ok {
					found = true
					break
				}
			}

			// check for reserved attribute name, skip processing if not present
			if !found {
				return nil
			}

			var ptr s3Pointer

			// attempt to unmarshal the message body into an S3 pointer
			err := jsonUnmarshal([]byte(*m.Body), &ptr)

			if err != nil {
				return fmt.Errorf("error when unmarshalling s3 pointer: %w", err)
			}

			// read the location of the S3 pointer to get full message
			s3Resp, err := c.s3c.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &ptr.S3BucketName,
				Key:    &ptr.S3Key,
			})

			if err != nil {
				return fmt.Errorf("error when reading from s3 (%s/%s): %w", ptr.S3BucketName, ptr.S3Key, err)
			}

			defer s3Resp.Body.Close()

			bodyBytes, err := io.ReadAll(s3Resp.Body)

			if err != nil {
				return fmt.Errorf("error when reading buffer: %w", err)
			}

			// override the body and receiptHandle on the original message
			sqsResp.Messages[i].Body = aws.String(string(bodyBytes))
			sqsResp.Messages[i].ReceiptHandle = aws.String(newExtendedReceiptHandle(ptr.S3BucketName, ptr.S3Key, *m.ReceiptHandle))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return sqsResp, nil
}

// RetrieveLambdaEvent is very similar to ReceiveMessage, but it operates on an already-fetched
// event (events.SQSEvent). This is meant to be used by those who need to interact with Extended SQS
// Messages that originate from a SQS -> Lambda event source. This function will fetch applicable S3
// messages for a provided [github.com/aws/aws-lambda-go/events.SQSEvent]. The provided SQSEvent
// will NOT be mutated, and a new SQSEvent will be returned that has a cloned Records array with any
// S3 Pointers resolved to their actual files.
//
// For each record in the provided event, if the configured Reserved Attribute Name
// ("ExtendedPayloadSize" by default) IS NOT present, the record is copied over without change to
// the returned event. However, if the Reserved Attribute Name IS present, the body of the record
// will be parsed to determine the S3 location of the full message body. This S3 location is read,
// and the body of the record will be overwritten with the contents. The last update is made to the
// record's ReceiptHandle, setting it to a unique pattern for the Extended SQS Client to be able to
// delete the S3 file when the SQS message is deleted (see [*Client.DeleteMessage] for more
// details).
func (c *Client) RetrieveLambdaEvent(ctx context.Context, evt *events.SQSEvent) (*events.SQSEvent, error) {
	g := new(errgroup.Group)
	copyRecords := make([]events.SQSMessage, len(evt.Records))

	for i, r := range evt.Records {
		i, r := i, r

		// always copy the entry, regardless of reserved attribute name
		copyRecords[i] = r

		g.Go(func() error {
			found := false
			for _, reservedAttr := range c.reservedAttrs {
				if _, ok := r.MessageAttributes[reservedAttr]; ok {
					found = true
					break
				}
			}

			// check for reserved attribute name, skip processing if not present
			if !found {
				return nil
			}

			var ptr s3Pointer
			err := jsonUnmarshal([]byte(r.Body), &ptr)

			if err != nil {
				return fmt.Errorf("error when unmarshalling s3 pointer: %w", err)
			}

			s3Resp, err := c.s3c.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &ptr.S3BucketName,
				Key:    &ptr.S3Key,
			})

			if err != nil {
				return fmt.Errorf("error when reading from s3 (%s/%s): %w", ptr.S3BucketName, ptr.S3Key, err)
			}

			defer s3Resp.Body.Close()

			bodyBytes, err := io.ReadAll(s3Resp.Body)

			if err != nil {
				return fmt.Errorf("error when reading buffer: %w", err)
			}

			copyRecords[i].Body = string(bodyBytes)
			copyRecords[i].ReceiptHandle = newExtendedReceiptHandle(ptr.S3BucketName, ptr.S3Key, r.ReceiptHandle)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	dup := *evt
	dup.Records = copyRecords
	return &dup, nil
}

// newExtendedReceiptHandle will return a properly formatted extended receipt handle given a bucket,
// key, and pre-existing handle
func newExtendedReceiptHandle(bucket, key, handle string) string {
	s3BucketNameMarker := "-..s3BucketName..-"
	s3KeyMarker := "-..s3Key..-"

	return fmt.Sprintf(
		"%s%s%s%s%s%s%s",
		s3BucketNameMarker,
		bucket,
		s3BucketNameMarker,
		s3KeyMarker,
		key,
		s3KeyMarker,
		handle,
	)
}

// extendedReceiptHandleRegex will extract the bucket, key, and existing handle from a properly
// formatted extended receipt handle
var extendedReceiptHandleRegex = regexp.MustCompile(`^-\.\.s3BucketName\.\.-(.*)-\.\.s3BucketName\.\.--\.\.s3Key\.\.-(.*)-\.\.s3Key\.\.-(.*)`)

// parseExtendedReceiptHandle will return a bucket, key, and existing handle from a provided
// extendedHandle. If the provided extendedHandle does not fit the expected format, empty strings
// are returned.
func parseExtendedReceiptHandle(extendedHandle string) (bucket, key, handle string) {
	match := extendedReceiptHandleRegex.FindStringSubmatch(extendedHandle)

	// we're expecting 3 matches; "first" match will be the entire string
	if len(match) != 4 {
		return "", "", ""
	}

	return match[1], match[2], match[3]
}

// DeleteMessage is a SQS Extended Client wrapper for the
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.DeleteMessage] function. If the provided
// params.ReceiptHandle matches with the format expected for the extended SQS client, it will be
// parsed and the linked S3 file will be deleted along with the actual SQS message.
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.DeleteMessage] for completeness:
//
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
	input := *params

	bucket, key, handle := parseExtendedReceiptHandle(*input.ReceiptHandle)
	if bucket != "" && key != "" && handle != "" {
		// override extended handle with actual sqs handle
		input.ReceiptHandle = &handle
	}

	resp, err := c.SQSClient.DeleteMessage(ctx, &input, optFns...)

	if err != nil {
		return nil, err
	}

	if !c.skipDeleteS3Payloads && bucket != "" && key != "" {
		_, err = c.s3c.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: &bucket,
			Key:    &key,
		})
	}

	return resp, err
}

// DeleteMessageBatch is a SQS Extended Client wrapper for the
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.DeleteMessageBatch] function. For each entry
// provided, if its ReceiptHandle matches with the format expected for the extended SQS client, it
// will be parsed and the linked S3 file will be deleted along with the actual SQS message.
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.DeleteMessageBatch] for
// completeness:
//
// Deletes up to ten messages from the specified queue. This is a batch version of DeleteMessage .
// The result of the action on each message is reported individually in the response. Because the
// batch request can result in a combination of successful and unsuccessful actions, you should
// check for batch errors even when the call returns an HTTP status code of 200.
func (c *Client) DeleteMessageBatch(ctx context.Context, params *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	input := *params
	copyEntries := make([]types.DeleteMessageBatchRequestEntry, len(input.Entries))
	// Map to track message IDs and their S3 information directly
	bucketKeysById := make(map[string]struct {
		bucket string
		key    string
	})

	for i, e := range input.Entries {
		// copy over the entry initially, regardless
		copyEntries[i] = e

		// check to see if ReceiptHandle fits extended format
		bucket, key, handle := parseExtendedReceiptHandle(*e.ReceiptHandle)
		if bucket != "" && key != "" && handle != "" {
			// Store the S3 object information with its message ID
			bucketKeysById[*e.Id] = struct {
				bucket string
				key    string
			}{
				bucket: bucket,
				key:    key,
			}

			// override the current entry to use new handle
			copyEntries[i].ReceiptHandle = &handle
		}
	}

	input.Entries = copyEntries
	resp, err := c.SQSClient.DeleteMessageBatch(ctx, &input, optFns...)

	if err != nil {
		return nil, err
	}

	// Only delete S3 objects for successfully deleted SQS messages
	g := new(errgroup.Group)

	// Create a map of successful message IDs for quick lookup
	successfulIds := make(map[string]bool)
	for _, entry := range resp.Successful {
		successfulIds[*entry.Id] = true
	}

	// Group S3 objects by bucket for efficient deletion
	bucketObjects := make(map[string][]s3types.ObjectIdentifier)

	// Only include objects for successfully deleted messages
	for id, obj := range bucketKeysById {
		if !successfulIds[id] {
			continue
		}

		if _, ok := bucketObjects[obj.bucket]; !ok {
			bucketObjects[obj.bucket] = []s3types.ObjectIdentifier{}
		}

		bucketObjects[obj.bucket] = append(bucketObjects[obj.bucket], s3types.ObjectIdentifier{
			Key: aws.String(obj.key),
		})
	}

	if !c.skipDeleteS3Payloads {
		// Delete objects in each bucket in parallel
		for bucket, objects := range bucketObjects {
			bucket, objects := bucket, objects
			g.Go(func() error {
				_, err := c.s3c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
					Bucket: &bucket,
					Delete: &s3types.Delete{Objects: objects},
				})
				return err
			})
		}
	}

	return resp, g.Wait()
}

// ChangeMessageVisibility is a SQS Extended Client wrapper for the
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.ChangeMessageVisibility] function. If the provided
// params.ReceiptHandle matches with the format expected for the extended SQS client, it will be
// parsed and the original SQS client method will be called.
//
// AWS doc for [github.com/aws/aws-sdk-go-v2/service/sqs.Client.ChangeMessageVisibility] for
// completeness:
//
// Changes the visibility timeout of a specified message in a queue to a new
// value. The default visibility timeout for a message is 30 seconds. The minimum
// is 0 seconds. The maximum is 12 hours. For more information, see Visibility
// Timeout (https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html)
// in the Amazon SQS Developer Guide. For example, if the default timeout for a
// queue is 60 seconds, 15 seconds have elapsed since you received the message, and
// you send a ChangeMessageVisibility call with VisibilityTimeout set to 10
// seconds, the 10 seconds begin to count from the time that you make the
// ChangeMessageVisibility call. Thus, any attempt to change the visibility timeout
// or to delete that message 10 seconds after you initially change the visibility
// timeout (a total of 25 seconds) might result in an error. An Amazon SQS message
// has three basic states:
//   - Sent to a queue by a producer.
//   - Received from the queue by a consumer.
//   - Deleted from the queue.
//
// A message is considered to be stored after it is sent to a queue by a producer,
// but not yet received from the queue by a consumer (that is, between states 1 and
// 2). There is no limit to the number of stored messages. A message is considered
// to be in flight after it is received from a queue by a consumer, but not yet
// deleted from the queue (that is, between states 2 and 3). There is a limit to
// the number of in flight messages. Limits that apply to in flight messages are
// unrelated to the unlimited number of stored messages. For most standard queues
// (depending on queue traffic and message backlog), there can be a maximum of
// approximately 120,000 in flight messages (received from a queue by a consumer,
// but not yet deleted from the queue). If you reach this limit, Amazon SQS returns
// the OverLimit error message. To avoid reaching the limit, you should delete
// messages from the queue after they're processed. You can also increase the
// number of queues you use to process your messages. To request a limit increase,
// file a support request (https://console.aws.amazon.com/support/home#/case/create?issueType=service-limit-increase&limitType=service-code-sqs)
// . For FIFO queues, there can be a maximum of 20,000 in flight messages (received
// from a queue by a consumer, but not yet deleted from the queue). If you reach
// this limit, Amazon SQS returns no error messages. If you attempt to set the
// VisibilityTimeout to a value greater than the maximum time left, Amazon SQS
// returns an error. Amazon SQS doesn't automatically recalculate and increase the
// timeout to the maximum remaining time. Unlike with a queue, when you change the
// visibility timeout for a specific message the timeout value is applied
// immediately but isn't saved in memory for that message. If you don't delete a
// message after it is received, the visibility timeout for the message reverts to
// the original timeout value (not to the value you set using the
// ChangeMessageVisibility action) the next time the message is received.
func (c *Client) ChangeMessageVisibility(ctx context.Context, params *sqs.ChangeMessageVisibilityInput, optFns ...func(*sqs.Options)) (*sqs.ChangeMessageVisibilityOutput, error) {
	input := *params

	bucket, key, handle := parseExtendedReceiptHandle(*input.ReceiptHandle)
	if bucket != "" && key != "" && handle != "" {
		// override extended handle with actual sqs handle
		input.ReceiptHandle = &handle
	}

	return c.SQSClient.ChangeMessageVisibility(ctx, &input, optFns...)
}
