package storage

import (
	"io"
)

// TODO: Use tags for specifying what database and collection we belong to.
type Saver interface {
	Save(path string, r io.Reader, tags map[string]string) error
}

// Fetcher should stream ReaderTaggers for all objects recursively in the specified path
type Fetcher interface {
	Fetch(path string) (<-chan ReadCloserTagger, error)
}

type Pather interface {
	Path() string
}

type Tagger interface {
	Tags() map[string]string
}

type ReadCloserTagger interface {
	io.ReadCloser
	Tagger
}
