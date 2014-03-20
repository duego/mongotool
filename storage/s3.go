package storage

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/smartystreets/go-aws-auth"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

type requestBuilder func(bucket, path string, body io.Reader, tags map[string]string) (req *http.Request, err error)

type s3File struct {
	bytes.Buffer
	path    string
	bucket  string
	builder requestBuilder
	closed  bool
}

func newS3File(bucket, path string, builder requestBuilder) *s3File {
	sf := s3File{
		bucket:  bucket,
		path:    path,
		builder: builder,
	}
	return &sf
}

func (sf *s3File) Close() error {
	if sf.closed {
		return nil
	}
	sf.closed = true

	req, err := sf.builder(sf.bucket, sf.path, bytes.NewReader(sf.Bytes()), nil)
	if err != nil {
		return err
	}
	client := http.DefaultClient

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if code := resp.StatusCode; code != 200 {
		msg, _ := ioutil.ReadAll(resp.Body)
		return errors.New(
			fmt.Sprintf("Expected 200 OK, got: (%d)\n%s", code, string(msg)),
		)
	}

	return nil
}

// S3 saves objects using Amazon services
type S3 struct {
	// The full path to the bucket host.
	// Example: https://mongotool.s3.amazonaws.com
	Bucket string
}

// checkAwsKeys will look for they environment variables implicitly used by go-aws-auth
func (s S3) checkAwsKeys() error {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		return errors.New("Missing AWS_ACCESS_KEY_ID environment variable")
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		return errors.New("Missing AWS_SECRET_ACCESS_KEY environment variable")
	}
	return nil
}

func (s S3) Save(path string) (io.WriteCloser, error) {
	if err := s.checkAwsKeys(); err != nil {
		return nil, err
	}
	return newS3File(s.Bucket, path, S3Object), nil
}

func S3Object(bucket, path string, body io.Reader, tags map[string]string) (req *http.Request, err error) {
	if len(path) > 0 {
		if string(path[0]) != "/" && string(bucket[len(bucket)]) != "/" {
			path = "/" + path
		}
	}
	if req, err = http.NewRequest("PUT", bucket+path, body); err != nil {
		return
	}
	for key, value := range tags {
		req.Header.Add("x-amz-meta-"+key, value)
	}

	awsauth.Sign4(req)
	return
}
