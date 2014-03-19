package storage

import (
	"io"
)

// TODO: Use tags for specifying what database and collection we belong to.
type Saver interface {
	Save(path string, r io.Reader, tags map[string]string) error
}

type Fetcher interface {
	Fetch(path string) <-chan ReaderTagger
}

type Tagger interface {
	Tags() map[string]string
}

type ReaderTagger interface {
	io.Reader
	Tagger
}
