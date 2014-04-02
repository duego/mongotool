package storage

import (
	"bytes"
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"strings"
	"testing"
)

var resource = os.Getenv("TestS3Object")

func TestS3(t *testing.T) {
	Convey("Given a real S3 bucket resource", t, func() {
		if resource == "" {
			SkipSo("TestS3Object flag not set")
			return
		}
		u, err := url.Parse(resource)
		if err != nil {
			SkipSo("S3PutObject URL invalid")
		}
		store := S3{fmt.Sprintf("%s://%s", u.Scheme, u.Host)}
		Convey("Our storage implements the Saver interface", func() {
			saver := Saver(store)
			So(saver, ShouldNotBeNil)
		})
		Convey("A request to PUT an object should succeed", func() {
			body := bytes.NewReader([]byte("Foo"))
			w, err := store.Save(u.Path)
			So(err, ShouldBeNil)
			_, err = io.Copy(w, body)
			So(err, ShouldBeNil)
			err = w.Close()
			So(err, ShouldBeNil)
		})

		Convey("Our storage implements the Walker interface", func() {
			walker := Walker(store)
			So(walker, ShouldNotBeNil)
			Convey("A request to get list of objects should succeed", func() {
				total := 0
				err := walker.Walk(path.Dir(u.Path), func(p string, err error) error {
					So(err, ShouldBeNil)
					So(p, ShouldEqual, strings.TrimLeft(u.Path, "/"))
					total++
					return err
				})
				So(err, ShouldBeNil)
				So(total, ShouldEqual, 1)
			})
		})
		Convey("Our storage implements the Fetcher inteface", func() {
			fetcher := Fetcher(store)
			So(fetcher, ShouldNotBeNil)
			Convey("A request to get an object should succeed", func() {
				o, err := fetcher.Fetch(strings.TrimLeft(u.Path, "/"))
				So(err, ShouldBeNil)
				So(o, ShouldNotBeNil)
				b, err := ioutil.ReadAll(o)
				So(err, ShouldBeNil)
				So(string(b), ShouldEqual, "Foo")
				So(o.Close(), ShouldBeNil)
			})
		})
	})
	Convey("Listing more than 1000 objects should be possible", t, func() {
		SkipSo("Not implemented")
	})
}

func TestS3File(t *testing.T) {
	puts := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := httputil.DumpRequest(r, true)
		t.Log(string(b))
		if r.Method == "PUT" {
			puts++
		}
	}))
	defer ts.Close()

	builder := func(method, bucket, path string, body io.Reader) (req *http.Request, err error) {
		return http.NewRequest("PUT", ts.URL, body)
	}

	f := news3FileWriter("bucket", "path", builder)
	Convey("A new S3File", t, func() {
		Convey("Implements Writer", func() {
			w := io.WriteCloser(f)
			So(w, ShouldNotBeNil)
			_, err := w.Write([]byte("foo"))
			So(err, ShouldBeNil)
			Convey("Which will only buffer the bytes in memory", func() {
				So(puts, ShouldEqual, 0)
				So(f.Len(), ShouldEqual, 3)
			})
		})
		Convey("And Closer", func() {
			c := io.Closer(f)
			So(c, ShouldNotBeNil)
			Convey("Which will PUT the written data once closed", func() {
				err := c.Close()
				So(err, ShouldBeNil)
				So(puts, ShouldEqual, 1)
			})
		})
	})
}
