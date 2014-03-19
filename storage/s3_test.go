package storage

import (
	"bytes"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"net/url"
	"os"
	"testing"
)

var resource = os.Getenv("TestS3Object")

func TestSignS3(t *testing.T) {
	Convey("Given a real S3 bucket resource", t, func() {
		if resource == "" {
			SkipSo("TestS3Object flag not set")
			return
		}
		u, err := url.Parse(resource)
		if err != nil {
			SkipSo("S3PutObject URL invalid")
		}
		Convey("A request to PUT an object should succeed", func() {
			body := []byte("Foo")

			store := S3{fmt.Sprintf("%s://%s", u.Scheme, u.Host)}
			err := store.Save(
				u.Path,
				bytes.NewReader(body),
				map[string]string{
					"Tag": "Test",
				},
			)
			if err != nil {
				t.Error(err)
			}

			So(err, ShouldBeNil)
		})
		Convey("A request to GET an object should succeed", func() {
			SkipSo("Not implemented")
		})
	})
}
