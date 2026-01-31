package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	urlpkg "net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/google/go-cmp/cmp"
	"gotest.tools/v3/assert"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

type mockS3API struct {
	HeadObjectFn          func(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	ListObjectVersionsFn  func(ctx context.Context, params *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error)
	ListObjectsV2Fn       func(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	ListObjectsFn         func(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error)
	CopyObjectFn          func(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	GetObjectFn           func(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	SelectObjectContentFn func(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error)
	DeleteObjectFn        func(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjectsFn       func(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListBucketsFn         func(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
	CreateBucketFn        func(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	DeleteBucketFn        func(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error)
	PutBucketVersioningFn func(ctx context.Context, params *s3.PutBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error)
	GetBucketVersioningFn func(ctx context.Context, params *s3.GetBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error)
	HeadBucketFn          func(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	GetBucketLocationFn   func(ctx context.Context, params *s3.GetBucketLocationInput, optFns ...func(*s3.Options)) (*s3.GetBucketLocationOutput, error)
}

func (m mockS3API) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.HeadObjectFn != nil {
		return m.HeadObjectFn(ctx, params, optFns...)
	}
	return &s3.HeadObjectOutput{}, nil
}

func (m mockS3API) ListObjectVersions(ctx context.Context, params *s3.ListObjectVersionsInput, optFns ...func(*s3.Options)) (*s3.ListObjectVersionsOutput, error) {
	if m.ListObjectVersionsFn != nil {
		return m.ListObjectVersionsFn(ctx, params, optFns...)
	}
	return &s3.ListObjectVersionsOutput{}, nil
}

func (m mockS3API) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.ListObjectsV2Fn != nil {
		return m.ListObjectsV2Fn(ctx, params, optFns...)
	}
	return &s3.ListObjectsV2Output{}, nil
}

func (m mockS3API) ListObjects(ctx context.Context, params *s3.ListObjectsInput, optFns ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
	if m.ListObjectsFn != nil {
		return m.ListObjectsFn(ctx, params, optFns...)
	}
	return &s3.ListObjectsOutput{}, nil
}

func (m mockS3API) CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
	if m.CopyObjectFn != nil {
		return m.CopyObjectFn(ctx, params, optFns...)
	}
	return &s3.CopyObjectOutput{}, nil
}

func (m mockS3API) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.GetObjectFn != nil {
		return m.GetObjectFn(ctx, params, optFns...)
	}
	return &s3.GetObjectOutput{}, nil
}

func (m mockS3API) SelectObjectContent(ctx context.Context, params *s3.SelectObjectContentInput, optFns ...func(*s3.Options)) (*s3.SelectObjectContentOutput, error) {
	if m.SelectObjectContentFn != nil {
		return m.SelectObjectContentFn(ctx, params, optFns...)
	}
	return &s3.SelectObjectContentOutput{}, nil
}

func (m mockS3API) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.DeleteObjectFn != nil {
		return m.DeleteObjectFn(ctx, params, optFns...)
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (m mockS3API) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if m.DeleteObjectsFn != nil {
		return m.DeleteObjectsFn(ctx, params, optFns...)
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (m mockS3API) ListBuckets(ctx context.Context, params *s3.ListBucketsInput, optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error) {
	if m.ListBucketsFn != nil {
		return m.ListBucketsFn(ctx, params, optFns...)
	}
	return &s3.ListBucketsOutput{}, nil
}

func (m mockS3API) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	if m.CreateBucketFn != nil {
		return m.CreateBucketFn(ctx, params, optFns...)
	}
	return &s3.CreateBucketOutput{}, nil
}

func (m mockS3API) DeleteBucket(ctx context.Context, params *s3.DeleteBucketInput, optFns ...func(*s3.Options)) (*s3.DeleteBucketOutput, error) {
	if m.DeleteBucketFn != nil {
		return m.DeleteBucketFn(ctx, params, optFns...)
	}
	return &s3.DeleteBucketOutput{}, nil
}

func (m mockS3API) PutBucketVersioning(ctx context.Context, params *s3.PutBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.PutBucketVersioningOutput, error) {
	if m.PutBucketVersioningFn != nil {
		return m.PutBucketVersioningFn(ctx, params, optFns...)
	}
	return &s3.PutBucketVersioningOutput{}, nil
}

func (m mockS3API) GetBucketVersioning(ctx context.Context, params *s3.GetBucketVersioningInput, optFns ...func(*s3.Options)) (*s3.GetBucketVersioningOutput, error) {
	if m.GetBucketVersioningFn != nil {
		return m.GetBucketVersioningFn(ctx, params, optFns...)
	}
	return &s3.GetBucketVersioningOutput{}, nil
}

func (m mockS3API) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if m.HeadBucketFn != nil {
		return m.HeadBucketFn(ctx, params, optFns...)
	}
	return &s3.HeadBucketOutput{}, nil
}

type mockUploader struct {
	UploadFn func(ctx context.Context, input *manager.UploadInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

func (m mockUploader) Upload(ctx context.Context, input *manager.UploadInput, optFns ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
	if m.UploadFn != nil {
		return m.UploadFn(ctx, input, optFns...)
	}
	return &manager.UploadOutput{}, nil
}

func TestS3ImplementsStorageInterface(t *testing.T) {
	var i interface{} = new(S3)
	if _, ok := i.(Storage); !ok {
		t.Errorf("expected %t to implement Storage interface", i)
	}
}

func TestNewSessionPathStyle(t *testing.T) {
	testcases := []struct {
		name            string
		endpoint        urlpkg.URL
		expectPathStyle bool
	}{
		{
			name:            "expect_virtual_host_style_when_missing_endpoint",
			endpoint:        urlpkg.URL{},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_transfer_accel",
			endpoint:        urlpkg.URL{Scheme: "https", Host: transferAccelEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_virtual_host_style_for_google_cloud_storage",
			endpoint:        urlpkg.URL{Scheme: "https", Host: gcsEndpoint},
			expectPathStyle: false,
		},
		{
			name:            "expect_path_style_for_localhost",
			endpoint:        urlpkg.URL{Scheme: "http", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_secure_localhost",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "127.0.0.1"},
			expectPathStyle: true,
		},
		{
			name:            "expect_path_style_for_custom_endpoint",
			endpoint:        urlpkg.URL{Scheme: "https", Host: "example.com"},
			expectPathStyle: true,
		},
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := !isVirtualHostStyle(tc.endpoint)
			if got != tc.expectPathStyle {
				t.Fatalf("expected: %v, got: %v", tc.expectPathStyle, got)
			}
		})
	}
}

func TestNewSessionWithRegionSetViaEnv(t *testing.T) {
	globalSessionCache.clear()

	const expectedRegion = "us-west-2"

	os.Setenv("AWS_REGION", expectedRegion)
	defer os.Unsetenv("AWS_REGION")

	cfg, err := globalSessionCache.newSession(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}

	got := cfg.Region
	if got != expectedRegion {
		t.Fatalf("expected %v, got %v", expectedRegion, got)
	}
}

func TestNewSessionWithNoSignRequest(t *testing.T) {
	globalSessionCache.clear()

	cfg, err := globalSessionCache.newSession(context.Background(), Options{
		NoSignRequest: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if creds.Source != "AnonymousCredentials" {
		t.Fatalf("expected %v, got %v", "AnonymousCredentials", creds.Source)
	}
}

func TestNewSessionWithProfileFromFile(t *testing.T) {
	// create a temporary credentials file
	file, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())

	profiles := `[default]
aws_access_key_id = default_profile_key_id
aws_secret_access_key = default_profile_access_key

[p1]
aws_access_key_id = p1_profile_key_id
aws_secret_access_key = p1_profile_access_key

[p2]
aws_access_key_id = p2_profile_key_id
aws_secret_access_key = p2_profile_access_key`

	_, err = file.Write([]byte(profiles))
	if err != nil {
		t.Fatal(err)
	}

	testcases := []struct {
		name               string
		fileName           string
		profileName        string
		expAccessKeyID     string
		expSecretAccessKey string
	}{
		{
			name:               "use default profile",
			fileName:           file.Name(),
			profileName:        "",
			expAccessKeyID:     "default_profile_key_id",
			expSecretAccessKey: "default_profile_access_key",
		},
		{
			name:               "use a non-default profile",
			fileName:           file.Name(),
			profileName:        "p1",
			expAccessKeyID:     "p1_profile_key_id",
			expSecretAccessKey: "p1_profile_access_key",
		},
		{
			name:               "use a non-existent profile",
			fileName:           file.Name(),
			profileName:        "non-existent-profile",
			expAccessKeyID:     "",
			expSecretAccessKey: "",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			cfg, err := globalSessionCache.newSession(context.Background(), Options{
				Profile:        tc.profileName,
				CredentialFile: tc.fileName,
			})
			if err != nil {
				t.Fatal(err)
			}

			got, err := cfg.Credentials.Retrieve(context.Background())
			if err != nil {
				// if there should be such a profile but received an error fail,
				// ignore the error otherwise.
				if tc.expAccessKeyID != "" || tc.expSecretAccessKey != "" {
					t.Fatal(err)
				}
			}

			if got.AccessKeyID != tc.expAccessKeyID || got.SecretAccessKey != tc.expSecretAccessKey {
				t.Errorf("Expected credentials does not match the credential we got!\nExpected: Access Key ID: %v, Secret Access Key: %v\nGot    : Access Key ID: %v, Secret Access Key: %v\n", tc.expAccessKeyID, tc.expSecretAccessKey, got.AccessKeyID, got.SecretAccessKey)
			}
		})
	}
}

func TestS3ListURL(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockAPI := mockS3API{
		ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("key/a/")},
					{Prefix: aws.String("key/b/")},
				},
				Contents: []types.Object{
					{Key: aws.String("key/test.txt")},
					{Key: aws.String("key/test.pdf")},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	mockS3 := &S3{api: mockAPI}

	responses := []struct {
		isDir  bool
		url    string
		relurl string
	}{
		{
			isDir:  true,
			url:    "s3://bucket/key/a/",
			relurl: "a/",
		},
		{
			isDir:  true,
			url:    "s3://bucket/key/b/",
			relurl: "b/",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.txt",
			relurl: "test.txt",
		},
		{
			isDir:  false,
			url:    "s3://bucket/key/test.pdf",
			relurl: "test.pdf",
		},
	}

	index := 0
	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != nil {
			t.Errorf("unexpected error: %v", got.Err)
			continue
		}

		want := responses[index]
		if diff := cmp.Diff(want.isDir, got.Type.IsDir()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.url, got.URL.Absolute()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		if diff := cmp.Diff(want.relurl, got.URL.Relative()); diff != "" {
			t.Errorf("(-want +got):\n%v", diff)
		}
		index++
	}
}

func TestS3ListError(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockErr := fmt.Errorf("mock error")
	mockAPI := mockS3API{
		ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return nil, mockErr
		},
	}
	mockS3 := &S3{api: mockAPI}

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != mockErr {
			t.Errorf("error got = %v, want %v", got.Err, mockErr)
		}
	}
}

func TestS3ListNoItemFound(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mockAPI := mockS3API{
		ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			// output does not include keys that match with given key
			return &s3.ListObjectsV2Output{
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("anotherkey/a/")},
					{Prefix: aws.String("anotherkey/b/")},
				},
				Contents: []types.Object{
					{Key: aws.String("a/b/c/d/test.txt")},
					{Key: aws.String("unknown/test.pdf")},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	mockS3 := &S3{api: mockAPI}

	for got := range mockS3.List(context.Background(), url, true) {
		if got.Err != ErrNoObjectFound {
			t.Errorf("error got = %v, want %v", got.Err, ErrNoObjectFound)
		}
	}
}

func TestS3ListContextCancelled(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	mockAPI := mockS3API{
		ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return &s3.ListObjectsV2Output{
				CommonPrefixes: []types.CommonPrefix{
					{Prefix: aws.String("key/a/")},
				},
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	mockS3 := &S3{api: mockAPI}

	for got := range mockS3.List(ctx, url, true) {
		if !errors.Is(got.Err, context.Canceled) {
			t.Errorf("error got = %v, want %v", got.Err, context.Canceled)
		}
	}
}

func TestS3RetryOnNoSuchUpload(t *testing.T) {
	log.Init("debug", false)

	noSuchUploadError := &smithy.GenericAPIError{
		Code:    "NoSuchUpload",
		Message: "The specified upload does not exist.",
	}
	testcases := []struct {
		name       string
		err        error
		retryCount int32
	}{
		{
			name:       "Don't retry",
			err:        noSuchUploadError,
			retryCount: 0,
		}, {
			name:       "Retry 5 times on NoSuchUpload error",
			err:        noSuchUploadError,
			retryCount: 5,
		}, {
			name:       "No error",
			err:        nil,
			retryCount: 0,
		},
	}

	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			uploadAttempts := int32(0)
			mockAPI := mockS3API{
				HeadObjectFn: func(ctx context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
					return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "not found"}
				},
			}
			mockS3 := &S3{
				api: mockAPI,
				uploader: mockUploader{
					UploadFn: func(ctx context.Context, input *manager.UploadInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
						atomic.AddInt32(&uploadAttempts, 1)
						return nil, tc.err
					},
				},
				noSuchUploadRetryCount: int(tc.retryCount),
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			mockS3.Put(ctx, strings.NewReader(""), url, Metadata{}, manager.DefaultUploadConcurrency, manager.DefaultUploadPartSize)

			want := tc.retryCount + 1
			if got := atomic.LoadInt32(&uploadAttempts); got != want {
				t.Errorf("expected upload attempts %d, got %d", want, got)
			}
		})
	}
}

func TestS3CopyEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{
			name: "no encryption/no acl, by default",
		},
		{
			name: "aws:kms encryption with server side generated keys",
			sse:  "aws:kms",

			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedACL: "bucket-owner-full-control",
		},
	}

	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var captured *s3.CopyObjectInput
			mockAPI := mockS3API{
				CopyObjectFn: func(ctx context.Context, params *s3.CopyObjectInput, _ ...func(*s3.Options)) (*s3.CopyObjectOutput, error) {
					captured = params
					return &s3.CopyObjectOutput{}, nil
				},
			}
			mockS3 := &S3{api: mockAPI}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Copy(context.Background(), u, u, metadata)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			if tc.expectedSSE == "" {
				assert.Equal(t, types.ServerSideEncryption(""), captured.ServerSideEncryption)
			} else {
				assert.Equal(t, types.ServerSideEncryption(tc.expectedSSE), captured.ServerSideEncryption)
			}

			if tc.expectedSSEKeyID == "" {
				assert.Equal(t, (*string)(nil), captured.SSEKMSKeyId)
			} else {
				assert.Equal(t, tc.expectedSSEKeyID, aws.ToString(captured.SSEKMSKeyId))
			}

			if tc.expectedACL == "" {
				assert.Equal(t, types.ObjectCannedACL(""), captured.ACL)
			} else {
				assert.Equal(t, types.ObjectCannedACL(tc.expectedACL), captured.ACL)
			}
		})
	}
}

func TestS3PutEncryptionRequest(t *testing.T) {
	testcases := []struct {
		name     string
		sse      string
		sseKeyID string
		acl      string

		expectedSSE      string
		expectedSSEKeyID string
		expectedACL      string
	}{
		{
			name: "no encryption, no acl flag",
		},
		{
			name:        "aws:kms encryption with server side generated keys",
			sse:         "aws:kms",
			expectedSSE: "aws:kms",
		},
		{
			name:     "aws:kms encryption with user provided key",
			sse:      "aws:kms",
			sseKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",

			expectedSSE:      "aws:kms",
			expectedSSEKeyID: "sdkjn12SDdci#@#EFRFERTqW/ke",
		},
		{
			name:     "provide key without encryption flag, shall be ignored",
			sseKeyID: "1234567890",
		},
		{
			name:        "acl flag with a value",
			acl:         "bucket-owner-full-control",
			expectedACL: "bucket-owner-full-control",
		},
	}
	u, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var captured *manager.UploadInput
			mockS3 := &S3{
				uploader: mockUploader{
					UploadFn: func(ctx context.Context, input *manager.UploadInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) {
						captured = input
						return &manager.UploadOutput{}, nil
					},
				},
			}

			metadata := Metadata{}
			metadata.EncryptionMethod = tc.sse
			metadata.EncryptionKeyID = tc.sseKeyID
			metadata.ACL = tc.acl

			err = mockS3.Put(context.Background(), bytes.NewReader([]byte("")), u, metadata, 1, 5242880)
			if err != nil {
				t.Errorf("Expected %v, but received %q", nil, err)
			}

			if tc.expectedSSE == "" {
				assert.Equal(t, types.ServerSideEncryption(""), captured.ServerSideEncryption)
			} else {
				assert.Equal(t, types.ServerSideEncryption(tc.expectedSSE), captured.ServerSideEncryption)
			}

			if tc.expectedSSEKeyID == "" {
				assert.Equal(t, (*string)(nil), captured.SSEKMSKeyId)
			} else {
				assert.Equal(t, tc.expectedSSEKeyID, aws.ToString(captured.SSEKMSKeyId))
			}

			if tc.expectedACL == "" {
				assert.Equal(t, types.ObjectCannedACL(""), captured.ACL)
			} else {
				assert.Equal(t, types.ObjectCannedACL(tc.expectedACL), captured.ACL)
			}
		})
	}
}

func TestS3listObjectsV2(t *testing.T) {
	const (
		numObjectsToReturn = 10100
		numObjectsToIgnore = 1127

		pre = "s3://bucket/key"
	)

	u, err := url.New(pre)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mapReturnObjNameToModtime := map[string]time.Time{}
	mapIgnoreObjNameToModtime := map[string]time.Time{}

	s3objs := make([]types.Object, 0, numObjectsToIgnore+numObjectsToReturn)

	for i := 0; i < numObjectsToReturn; i++ {
		fname := fmt.Sprintf("%s/%d", pre, i)
		now := time.Now()

		mapReturnObjNameToModtime[pre+"/"+fname] = now
		s3objs = append(s3objs, types.Object{
			Key:          aws.String("key/" + fname),
			LastModified: &now,
		})
	}

	for i := 0; i < numObjectsToIgnore; i++ {
		fname := fmt.Sprintf("%s/%d", pre, numObjectsToReturn+i)
		later := time.Now().Add(time.Second * 10)

		mapIgnoreObjNameToModtime[pre+"/"+fname] = later
		s3objs = append(s3objs, types.Object{
			Key:          aws.String("key/" + fname),
			LastModified: &later,
		})
	}

	// shuffle the objects array to remove possible assumptions about how objects
	// are stored.
	rand.Shuffle(len(s3objs), func(i, j int) {
		s3objs[i], s3objs[j] = s3objs[j], s3objs[i]
	})

	mockAPI := mockS3API{
		ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
			return &s3.ListObjectsV2Output{
				Contents:    s3objs,
				IsTruncated: aws.Bool(false),
			}, nil
		},
	}
	mockS3 := &S3{api: mockAPI}

	ouputCh := mockS3.listObjectsV2(context.Background(), u)

	for obj := range ouputCh {
		if _, ok := mapReturnObjNameToModtime[obj.String()]; ok {
			delete(mapReturnObjNameToModtime, obj.String())
			continue
		}
		t.Errorf("%v should not have been returned\n", obj)
	}
	assert.Equal(t, len(mapReturnObjNameToModtime), 0)
}

func TestSessionCreateAndCachingWithDifferentBuckets(t *testing.T) {
	log.Init("error", false)
	testcases := []struct {
		bucket         string
		alreadyCreated bool // sessions should not be created again if they already have been created before
	}{
		{bucket: "bucket"},
		{bucket: "bucket", alreadyCreated: true},
		{bucket: "test-bucket"},
	}

	sess := map[string]aws.Config{}

	for _, tc := range testcases {
		awsSess, err := globalSessionCache.newSession(context.Background(), Options{
			bucket: tc.bucket,
		})
		if err != nil {
			t.Error(err)
		}

		if tc.alreadyCreated {
			_, ok := sess[tc.bucket]
			assert.Check(t, ok, "session should not have been created again")
		} else {
			sess[tc.bucket] = awsSess
		}
	}
}

func TestSessionRegionDetection(t *testing.T) {
	testcases := []struct {
		name           string
		optsRegion     string
		envRegion      string
		expectedRegion string
	}{
		{
			name:           "RegionWithSourceRegionParameter",
			optsRegion:     "ap-east-1",
			envRegion:      "ca-central-1",
			expectedRegion: "ap-east-1",
		},
		{
			name:           "RegionWithEnvironmentVariable",
			optsRegion:     "",
			envRegion:      "ca-central-1",
			expectedRegion: "ca-central-1",
		},
		{
			name:           "DefaultRegion",
			optsRegion:     "",
			envRegion:      "",
			expectedRegion: "us-east-1",
		},
	}

	// ignore local profile loading
	os.Setenv("AWS_SDK_LOAD_CONFIG", "0")

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			opts := Options{
				// since profile loading disabled above, we need to provide
				// credentials to the session. NoSignRequest could be used
				// for anonymous credentials.
				NoSignRequest: true,
			}

			if tc.optsRegion != "" {
				opts.region = tc.optsRegion
			}

			if tc.envRegion != "" {
				os.Setenv("AWS_REGION", tc.envRegion)
				defer os.Unsetenv("AWS_REGION")
			}

			globalSessionCache.clear()

			sess, err := globalSessionCache.newSession(context.Background(), opts)
			if err != nil {
				t.Fatal(err)
			}

			got := sess.Region
			if got != tc.expectedRegion {
				t.Fatalf("expected %v, got %v", tc.expectedRegion, got)
			}
		})
	}
}

func TestS3ListObjectsAPIVersions(t *testing.T) {
	url, err := url.New("s3://bucket/key")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	t.Run("list-objects-v2", func(t *testing.T) {
		calledV2 := 0
		mockAPI := mockS3API{
			ListObjectsV2Fn: func(ctx context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
				calledV2++
				return &s3.ListObjectsV2Output{IsTruncated: aws.Bool(false)}, nil
			},
		}
		mockS3 := &S3{api: mockAPI}

		ctx := context.Background()
		mockS3.useListObjectsV1 = false
		for range mockS3.List(ctx, url, false) {
		}

		if calledV2 == 0 {
			t.Errorf("expected listObjectsV2 to be called")
		}
	})

	t.Run("list-objects-v1", func(t *testing.T) {
		calledV1 := 0
		mockAPI := mockS3API{
			ListObjectsFn: func(ctx context.Context, params *s3.ListObjectsInput, _ ...func(*s3.Options)) (*s3.ListObjectsOutput, error) {
				calledV1++
				return &s3.ListObjectsOutput{IsTruncated: aws.Bool(false)}, nil
			},
		}
		mockS3 := &S3{api: mockAPI}

		ctx := context.Background()
		mockS3.useListObjectsV1 = true
		for range mockS3.List(ctx, url, false) {
		}

		if calledV1 == 0 {
			t.Errorf("expected listObjectsV1 to be called")
		}
	})
}

func TestAWSLogLevel(t *testing.T) {
	testcases := []struct {
		name     string
		level    string
		expected aws.ClientLogMode
	}{
		{
			name:     "Trace: log level must enable request/response logging",
			level:    "trace",
			expected: aws.LogRequestWithBody | aws.LogResponseWithBody,
		},
		{
			name:     "Debug: log level must be aws.LogOff",
			level:    "debug",
			expected: aws.LogOff,
		},
		{
			name:     "Info: log level must be aws.LogOff",
			level:    "info",
			expected: aws.LogOff,
		},
		{
			name:     "Error: log level must be aws.LogOff",
			level:    "error",
			expected: aws.LogOff,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			globalSessionCache.clear()
			sess, err := globalSessionCache.newSession(context.Background(), Options{
				LogLevel: log.LevelFromString(tc.level),
			})
			if err != nil {
				t.Fatal(err)
			}

			cfgLogLevel := sess.ClientLogMode
			if diff := cmp.Diff(cfgLogLevel, tc.expected); diff != "" {
				t.Errorf("%s: (-want +got):\n%v", tc.name, diff)
			}
		})
	}
}

func TestS3HeadObject(t *testing.T) {
	testcases := []struct {
		name     string
		url      string
		expected string
	}{
		{
			name:     "HeadObject",
			url:      "s3://bucket/key",
			expected: "bucket/key",
		},
		{
			name:     "HeadObject with different URL",
			url:      "s3://another-bucket/another-key",
			expected: "another-bucket/another-key",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.New(tc.url)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			mockAPI := mockS3API{
				HeadObjectFn: func(ctx context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
					return &s3.HeadObjectOutput{}, nil
				},
			}
			mockS3 := &S3{api: mockAPI}

			_, _, err = mockS3.HeadObject(context.Background(), u)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}
