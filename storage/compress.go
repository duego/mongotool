package storage

import (
	"compress/gzip"
	"io"
)

type gzipFile struct {
	*gzip.Writer
	original io.WriteCloser
}

func (g *gzipFile) Close() error {
	if err := g.Writer.Close(); err != nil {
		g.original.Close()
		return err
	}
	return g.original.Close()
}

type Gzip struct {
	s Saver
}

func NewGzip(s Saver) Saver {
	return &Gzip{s}
}

func (c *Gzip) Save(path string) (io.WriteCloser, error) {
	w, err := c.s.Save(path)
	if err != nil {
		return nil, err
	}
	return &gzipFile{gzip.NewWriter(w), w}, nil
}
