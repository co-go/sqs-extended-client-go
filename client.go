package sqsextendedclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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
)

var (
	jsonUnmarshal = json.Unmarshal
	jsonMarshal   = json.Marshal
)

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
}

// Client is a wrapper for the [github.com/aws/aws-sdk-go-v2/service/sqs.Client], providing extra
// functionality for retrieving, sending and deleting messages.
type Client struct {
	SQSClient
	s3c                  S3Client
	bucketName           string
	messageSizeThreshold int64
	alwaysThroughS3      bool
	pointerClass         string
	reservedAttrName     string
}

type ClientOption func(*Client) error

// New returns a newly created [*Client] with defaults:
//   - MessageSizeThreshold: 262144 (256 KiB)
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
		SQSClient:            sqsc,
		s3c:                  s3c,
		messageSizeThreshold: 262144, // 256 KiB
		pointerClass:         "software.amazon.payloadoffloading.PayloadS3Pointer",
		reservedAttrName:     "ExtendedPayloadSize",
	}

	// apply optFns to the base client
	for _, optFn := range optFns {
		err := optFn(&c)
		if err != nil {
			return nil, err
		}
	}

	return &c, nil
}

// Set the destination bucket for large messages that are sent by this client. This is a
// soft-requirement for using the SendMessage function.
func WithS3BucketName(bucketName string) ClientOption {
	return func(c *Client) error {
		c.bucketName = bucketName
		return nil
	}
}

// Set the MessageSizeThreshold to some other value (in bytes). By default this is 262144 (256
// KiB).
func WithMessageSizeThreshold(size int) ClientOption {
	return func(c *Client) error {
		c.messageSizeThreshold = int64(size)
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

// Override the ReservedAttributeName with a custom value (i.e.
// [LegacyReservedAttributeName])
func WithReservedAttributeName(attributeName string) ClientOption {
	return func(c *Client) error {
		c.reservedAttrName = attributeName
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

// messageExceedsThreshold determines if the size of the body and attributes exceeds the configured
// message size threshold
func (c *Client) messageExceedsThreshold(body *string, attributes map[string]types.MessageAttributeValue) bool {
	return int64(len(*body))+c.attributeSize(attributes) > c.messageSizeThreshold
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
// If the provided message exceeds the message size threshold (defaults to 256KiB), then the message
// will be uploaded to S3. Assuming a successful upload, the message will be altered by:
//
//  1. Adding a custom attribute under the configured reserved attribute name that contains the size
//     of the large payload.
//  2. Body of the original message overridden with a S3 Pointer to the newly created S3 location
//     that holds the entirety of the message
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

	if c.alwaysThroughS3 || c.messageExceedsThreshold(input.MessageBody, input.MessageAttributes) {
		// generate UUID filename
		s3Key := uuid.New().String()

		// upload large payload to S3
		_, err := c.s3c.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &c.bucketName,
			Key:    aws.String(s3Key),
			Body:   strings.NewReader(*input.MessageBody),
		})

		if err != nil {
			return nil, fmt.Errorf("unable to upload large payload to s3: %w", err)
		}

		// create an s3 pointer that will be uploaded to SQS in place of the large payload
		asBytes, err := jsonMarshal(&s3Pointer{
			S3BucketName: c.bucketName,
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
		updatedAttributes[c.reservedAttrName] = types.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(strconv.Itoa(len(*input.MessageBody))),
		}

		// override attributes and body in the original message
		input.MessageAttributes = updatedAttributes
		input.MessageBody = aws.String(string(asBytes))
	}

	return c.SQSClient.SendMessage(ctx, &input, optFns...)
}

// Extended SQS Client wrapper around
// [github.com/aws/aws-sdk-go-v2/service/sqs.Client.SendMessageBatch]. For each provided message in
// the batch, if the message exceeds the message size threshold (defaults to 256KiB), then the
// message will be uploaded to S3. Assuming a successful upload, the message will be altered by:
//
//  1. Adding a custom attribute under the configured reserved attribute name that contains the size
//     of the large payload.
//  2. Body of the original message overridden with a S3 Pointer to the newly created S3 location
//     that holds the entirety of the message
//
// After all applicable messages are uploaded to S3, then the SQS native SendMessageBatch call is
// invoked.
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
// sum of the individual lengths of all of the batched messages) are both 256 KiB (262,144 bytes). A
// message can include only XML, JSON, and unformatted text. The following Unicode characters are
// allowed: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF Any characters
// not included in this list will be rejected. For more information, see the W3C specification for
// characters (http://www.w3.org/TR/REC-xml/#charsets). If you don't specify the DelaySeconds
// parameter for an entry, Amazon SQS uses the default value for the queue.
func (c *Client) SendMessageBatch(ctx context.Context, params *sqs.SendMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error) {
	input := *params
	copyEntries := make([]types.SendMessageBatchRequestEntry, len(input.Entries))
	g := new(errgroup.Group)

	for i, e := range input.Entries {
		i, e := i, e

		// always copy the entry, regardless of size
		copyEntries[i] = e

		if c.alwaysThroughS3 || c.messageExceedsThreshold(e.MessageBody, e.MessageAttributes) {
			// generate UUID filename
			s3Key := uuid.New().String()

			// upload large payload to S3
			g.Go(func() error {
				_, err := c.s3c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: &c.bucketName,
					Key:    aws.String(s3Key),
					Body:   strings.NewReader(*e.MessageBody),
				})

				if err != nil {
					return fmt.Errorf("unable to upload large payload to s3: %w", err)
				}

				return nil
			})

			// create an s3 pointer that will be uploaded to SQS in place of the large payload
			asBytes, err := jsonMarshal(&s3Pointer{
				S3BucketName: c.bucketName,
				S3Key:        s3Key,
				class:        c.pointerClass,
			})

			if err != nil {
				return nil, fmt.Errorf("unable to marshal S3 pointer: %w", err)
			}

			// copy over all attributes, leaving space for our reserved attribute
			updatedAttributes := make(map[string]types.MessageAttributeValue, len(e.MessageAttributes)+1)
			for k, v := range e.MessageAttributes {
				updatedAttributes[k] = v
			}

			// assign the reserved attribute to a number containing the size of the original body
			updatedAttributes[c.reservedAttrName] = types.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String(strconv.Itoa(len(*e.MessageBody))),
			}

			// override attributes and body in the original message
			copyEntries[i].MessageAttributes = updatedAttributes
			copyEntries[i].MessageBody = aws.String(string(asBytes))
		}
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
// delete the S3 file when the SQS message is deleted (see [*Client.DeleteMessage] for more details).
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
	messageAttributeCopy := make([]string, len(input.MessageAttributeNames)+1)
	for i, a := range input.MessageAttributeNames {
		messageAttributeCopy[i] = a
		if a == "All" || a == ".*" {
			includesAll = true
		} else if a == c.reservedAttrName {
			includesAttributeName = true
		}
	}

	// if the reserved attribute name is not present, add it to the list
	if !includesAttributeName && !includesAll {
		messageAttributeCopy[len(input.MessageAttributeNames)] = c.reservedAttrName
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
			// check for reserved attribute name, skip processing if not present
			if _, ok := m.MessageAttributes[c.reservedAttrName]; !ok {
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
			// check for reserved attribute name, skip processing if not present
			if _, ok := r.MessageAttributes[c.reservedAttrName]; !ok {
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

	if bucket != "" && key != "" {
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
	deleteRequests := map[string]s3types.Delete{}

	for i, e := range input.Entries {
		// copy over the entry initially, regardless
		copyEntries[i] = e

		// check to see if ReceiptHandle fits extended format
		bucket, key, handle := parseExtendedReceiptHandle(*e.ReceiptHandle)
		if bucket != "" && key != "" && handle != "" {
			// if this is the first time we've seen a bucket, instantiate the array
			if _, ok := deleteRequests[bucket]; !ok {
				deleteRequests[bucket] = s3types.Delete{Objects: []s3types.ObjectIdentifier{}}
			}

			req := deleteRequests[bucket]

			// append the current key to the list of objects to be deleted
			req.Objects = append(req.Objects, s3types.ObjectIdentifier{Key: &key})
			deleteRequests[bucket] = req

			// override the current entry to use new handle
			copyEntries[i].ReceiptHandle = &handle
		}
	}

	input.Entries = copyEntries
	resp, err := c.SQSClient.DeleteMessageBatch(ctx, &input, optFns...)

	if err != nil {
		return nil, err
	}

	g := new(errgroup.Group)

	// for each delete request (grouped by bucket), send DeleteObjects call in parallel
	for b, d := range deleteRequests {
		b, d := b, d
		g.Go(func() error {
			_, err := c.s3c.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: &b,
				Delete: &d,
			})

			return err
		})
	}

	return resp, g.Wait()
}
