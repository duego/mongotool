package storage

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"io"
	"testing"
)

type fooStorage struct {
	*bytes.Buffer
}

func (f fooStorage) Save(path string) (io.WriteCloser, error) {
	return f, nil
}

func (f fooStorage) Fetch(path string) (io.ReadCloser, error) {
	return f, nil
}

func (f fooStorage) Close() error {
	return nil
}

func TestGzipSaver(t *testing.T) {
	Convey("Given an GzipSaveFetcher", t, func() {
		g := NewGzipSaveFetcher(new(fooStorage))
		Convey("We should implement saver", func() {
			_, ok := g.(Saver)
			So(ok, ShouldBeTrue)
		})
		Convey("We should implement fetcher", func() {
			_, ok := g.(Fetcher)
			So(ok, ShouldBeTrue)
		})
		Convey("We should implement walker", func() {
			_, ok := g.(Walker)
			So(ok, ShouldBeTrue)
		})
	})
}
