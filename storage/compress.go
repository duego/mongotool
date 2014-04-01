package storage

import (
	"compress/gzip"
	"io"
)

type gzipReadCloser struct {
	*gzip.Reader
	original io.ReadCloser
}

func (g *gzipReadCloser) Close() error {
	if err := g.Reader.Close(); err != nil {
		g.original.Close()
		return err
	}
	return g.original.Close()
}

type gzipWriteCloser struct {
	*gzip.Writer
	original io.WriteCloser
}

func (g *gzipWriteCloser) Close() error {
	if err := g.Writer.Close(); err != nil {
		g.original.Close()
		return err
	}
	return g.original.Close()
}

type GzipSaveFetcher struct {
	s SaveFetcher
}

func NewGzipSaveFetcher(s SaveFetcher) SaveFetcher {
	return &GzipSaveFetcher{s}
}

func (c *GzipSaveFetcher) Save(path string) (io.WriteCloser, error) {
	w, err := c.s.Save(path)
	if err != nil {
		return nil, err
	}
	return &gzipWriteCloser{gzip.NewWriter(w), w}, nil
}

func (c *GzipSaveFetcher) Fetch(path string) (io.ReadCloser, error) {
	r, err := c.s.Fetch(path)
	if err != nil {
		return nil, err
	}
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	return &gzipReadCloser{gr, r}, nil
}

func (c *GzipSaveFetcher) Walk(path string, walkfn WalkFunc) error {
	w := c.s.(Walker)
	return w.Walk(path, walkfn)
}
