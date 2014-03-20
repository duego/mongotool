package storage

import (
	"io"
)

type Saver interface {
	Save(path string) (io.WriteCloser, error)
}

// Fetcher should stream ReaderClosers for all objects recursively in the specified path
type Fetcher interface {
	Fetch(path string) (<-chan io.ReadCloser, error)
}

type Pather interface {
	Path() string
}

type Tagger interface {
	Tags() map[string]string
}
