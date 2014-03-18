package main

import (
	"bytes"
	"github.com/smartystreets/go-aws-auth"
	. "github.com/smartystreets/goconvey/convey"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
)

func TestSignS3(t *testing.T) {
	Convey("Given a real S3 bucket resource", t, func() {
		resource := os.Getenv("S3PutObject")
		if resource == "" {
			SkipSo("S3PutObject environment variable not set")
			return
		}
		Convey("A request to PUT an object should succeed", func() {
			body := []byte("Foo")
			req, err := http.NewRequest("PUT", resource, bytes.NewReader(body))
			if err != nil {
				t.Error(err)
			}

			awsauth.SignS3(req)
			rep, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
			} else {
				defer rep.Body.Close()
			}
			if rep.StatusCode != http.StatusOK {
				msg, _ := ioutil.ReadAll(rep.Body)
				t.Error(string(msg))
			}
			So(rep.StatusCode, ShouldEqual, http.StatusOK)
		})
		Convey("A request to GET an object should succeed", func() {
			req, err := http.NewRequest("GET", resource, nil)
			if err != nil {
				t.Error(err)
			}

			awsauth.SignS3(req)
			rep, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Error(err)
			} else {
				defer rep.Body.Close()
			}
			if rep.StatusCode != http.StatusOK {
				msg, _ := ioutil.ReadAll(rep.Body)
				t.Error(string(msg))
			}
			So(rep.StatusCode, ShouldEqual, http.StatusOK)
		})
	})
}
