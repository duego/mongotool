package storage

import (
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
)

type Filesystem struct {
	Root string
}

func (f Filesystem) Save(fpath string) (io.WriteCloser, error) {
	fullpath := path.Join(f.Root, fpath)
	if err := os.MkdirAll(path.Dir(fullpath), 0700); err != nil {
		return nil, err
	}
	fd, err := os.Create(fullpath)
	if err != nil {
		return nil, err
	}
	return fd, err
}

func (f Filesystem) Fetch(fpath string) (<-chan io.ReadCloser, error) {
	fullpath := path.Join(f.Root, fpath)

	c := make(chan io.ReadCloser)
	go func() {
		defer close(c)

		filepath.Walk(fullpath, func(fpath string, info os.FileInfo, err error) error {
			if err != nil {
				log.Println(err)
				return err
			}
			if info.IsDir() {
				return nil
			}
			o, err := os.Open(fpath)
			if err != nil {
				log.Println(err)
				return nil
			}

			log.Println(info.Name())
			c <- o
			return nil
		})
	}()

	return c, nil
}
