// Package cas implements the REAPI Content Addressable Store and Action Cache
// on top of an S3 bucket.
//
// Layout within the bucket (under an optional key prefix):
//
//	<prefix>/cas/<sha256>          - a CAS blob, keyed only by its hash
//	<prefix>/ac/<sha256>           - a serialized ActionResult, keyed by the
//	                                 action digest's hash
//
// Blobs are keyed by hash alone: SHA-256 is collision resistant, so two blobs
// with the same hash necessarily have the same size and contents.
package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	smithy "github.com/aws/smithy-go"
	"google.golang.org/protobuf/proto"

	"github.com/kyleconroy/aws-build-farm/internal/digest"
)

// ErrNotFound is returned when a blob or action result is absent.
var ErrNotFound = errors.New("cas: not found")

// Store is the subset of CAS behaviour the gRPC server and executor-agent rely
// on. It is satisfied by both the S3-backed store and the in-memory store used
// in tests.
type Store interface {
	Contains(ctx context.Context, d *repb.Digest) (bool, error)
	Get(ctx context.Context, d *repb.Digest) ([]byte, error)
	Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error)
	Put(ctx context.Context, d *repb.Digest, data []byte) error
	PutStream(ctx context.Context, d *repb.Digest, size int64, r io.Reader) error
	GetActionResult(ctx context.Context, actionDigest *repb.Digest) (*repb.ActionResult, error)
	PutActionResult(ctx context.Context, actionDigest *repb.Digest, ar *repb.ActionResult) error
}

// S3Store is an S3-backed implementation of Store.
type S3Store struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
	prefix   string
}

// NewS3Store returns a CAS backed by the given bucket and optional key prefix.
func NewS3Store(client *s3.Client, bucket, prefix string) *S3Store {
	return &S3Store{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
		prefix:   prefix,
	}
}

func (s *S3Store) blobKey(d *repb.Digest) string {
	return path.Join(s.prefix, "cas", d.GetHash())
}

func (s *S3Store) acKey(d *repb.Digest) string {
	return path.Join(s.prefix, "ac", d.GetHash())
}

func (s *S3Store) Contains(ctx context.Context, d *repb.Digest) (bool, error) {
	if digest.IsEmpty(d) {
		return true, nil
	}
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.blobKey(d)),
	})
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("head %s: %w", digest.Key(d), err)
	}
	return true, nil
}

func (s *S3Store) Get(ctx context.Context, d *repb.Digest) ([]byte, error) {
	if digest.IsEmpty(d) {
		return []byte{}, nil
	}
	rc, err := s.Reader(ctx, d, 0, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (s *S3Store) Reader(ctx context.Context, d *repb.Digest, offset, limit int64) (io.ReadCloser, error) {
	if digest.IsEmpty(d) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	in := &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.blobKey(d)),
	}
	if rng := byteRange(offset, limit); rng != "" {
		in.Range = aws.String(rng)
	}
	out, err := s.client.GetObject(ctx, in)
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get %s: %w", digest.Key(d), err)
	}
	return out.Body, nil
}

func (s *S3Store) Put(ctx context.Context, d *repb.Digest, data []byte) error {
	if digest.IsEmpty(d) {
		return nil
	}
	return s.PutStream(ctx, d, int64(len(data)), bytes.NewReader(data))
}

func (s *S3Store) PutStream(ctx context.Context, d *repb.Digest, size int64, r io.Reader) error {
	if digest.IsEmpty(d) {
		// Drain to keep callers honest, but nothing to store.
		_, _ = io.Copy(io.Discard, r)
		return nil
	}
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.blobKey(d)),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", digest.Key(d), err)
	}
	return nil
}

func (s *S3Store) GetActionResult(ctx context.Context, actionDigest *repb.Digest) (*repb.ActionResult, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.acKey(actionDigest)),
	})
	if err != nil {
		if isNotFound(err) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get action result %s: %w", digest.Key(actionDigest), err)
	}
	defer out.Body.Close()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}
	ar := &repb.ActionResult{}
	if err := proto.Unmarshal(data, ar); err != nil {
		return nil, fmt.Errorf("unmarshal action result: %w", err)
	}
	return ar, nil
}

func (s *S3Store) PutActionResult(ctx context.Context, actionDigest *repb.Digest, ar *repb.ActionResult) error {
	data, err := proto.Marshal(ar)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.acKey(actionDigest)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("put action result %s: %w", digest.Key(actionDigest), err)
	}
	return nil
}

// byteRange renders an HTTP Range header value for the given offset and limit.
// A limit of 0 means "to end of object". An offset of 0 with no limit returns
// "" so the whole object is fetched.
func byteRange(offset, limit int64) string {
	if offset <= 0 && limit <= 0 {
		return ""
	}
	if limit <= 0 {
		return fmt.Sprintf("bytes=%d-", offset)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+limit-1)
}

func isNotFound(err error) bool {
	var nsk *s3types.NoSuchKey
	var nf *s3types.NotFound
	if errors.As(err, &nsk) || errors.As(err, &nf) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "NoSuchKey", "NotFound", "404":
			return true
		}
	}
	return false
}
