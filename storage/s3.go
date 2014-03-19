package storage

import (
	"errors"
	"fmt"
	"github.com/smartystreets/go-aws-auth"
	"io"
	"io/ioutil"
	"net/http"
	"os"
)

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

func (s S3) Save(path string, object io.Reader, tags map[string]string) error {
	if err := s.checkAwsKeys(); err != nil {
		return err
	}
	req, err := s.S3Object(path, object, tags)
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

// TODO: Auth signature doesn't work, see test.
func (s S3) S3Object(path string, body io.Reader, tags map[string]string) (req *http.Request, err error) {
	if len(path) > 0 {
		if string(path[0]) != "/" {
			path = "/" + path
		}
	}
	if req, err = http.NewRequest("PUT", s.Bucket+path, body); err != nil {
		return
	}
	for key, value := range tags {
		req.Header.Add("x-amz-meta-"+key, value)
	}

	awsauth.SignS3(req)
	return
}
