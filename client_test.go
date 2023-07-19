package sqsextendedclient

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// assert not mutating inputs
func TestS3PointerMarshal(t *testing.T) {
	p := s3Pointer{
		S3BucketName: "some-bucket",
		S3Key:        "some-key",
		class:        "com.james.testing.Pointer",
	}

	asBytes, err := json.Marshal(&p)
	assert.Nil(t, err)
	assert.Equal(t, `["com.james.testing.Pointer",{"s3BucketName":"some-bucket","s3Key":"some-key"}]`, string(asBytes))
}

func TestS3PointerUnmarshal(t *testing.T) {
	str := []byte(`["com.james.testing.Pointer",{"s3BucketName":"some-bucket","s3Key":"some-key"}]`)

	var p s3Pointer
	err := json.Unmarshal(str, &p)
	assert.Nil(t, err)
	assert.Equal(t, s3Pointer{
		S3BucketName: "some-bucket",
		S3Key:        "some-key",
		class:        "com.james.testing.Pointer",
	}, p)
}

func TestS3PointerUnmarshalInvalidLength(t *testing.T) {
	str := []byte(`["com.james.testing.Pointer",{"s3BucketName":"some-bucket","s3Key":"some-key"}, "bonus!"]`)

	var p s3Pointer
	err := json.Unmarshal(str, &p)
	assert.ErrorContains(t, err, "invalid pointer format, expected length 2, but received [3]")
}

func TestParseExtendedReceiptHandle(t *testing.T) {
	bucket, key, handle := parseExtendedReceiptHandle("-..s3BucketName..-some-bucket-..s3BucketName..--..s3Key..-some-key-..s3Key..-abcdefg")
	assert.Equal(t, "some-bucket", bucket)
	assert.Equal(t, "some-key", key)
	assert.Equal(t, "abcdefg", handle)
}

func TestParseExtendedReceiptHandleFailure(t *testing.T) {
	bucket, key, handle := parseExtendedReceiptHandle("nonExtendedHandle")
	assert.Equal(t, "", bucket)
	assert.Equal(t, "", key)
	assert.Equal(t, "", handle)
}
