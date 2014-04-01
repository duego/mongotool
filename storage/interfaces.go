package storage

import (
	"io"
)

type Filer interface {
	io.Reader
	Pather
	Length() int64
}

type SaveFetcher interface {
	Saver
	Fetcher
}

type Saver interface {
	Save(path string) (io.WriteCloser, error)
}

type Fetcher interface {
	Fetch(path string) (io.ReadCloser, error)
}

type Pather interface {
	Path() string
}

type Tagger interface {
	Tags() map[string]string
}

type Walker interface {
	Walk(path string, walkfn WalkFunc) error
}

type WalkFunc func(fpath string, err error) error
