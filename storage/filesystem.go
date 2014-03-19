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

func (f Filesystem) Save(fpath string, object io.Reader, tags map[string]string) error {
	fullpath := path.Join(f.Root, fpath)
	if err := os.MkdirAll(path.Dir(fullpath), 0700); err != nil {
		return err
	}
	fd, err := os.Create(fullpath)
	if err != nil {
		return err
	}
	_, err = io.Copy(fd, object)
	return err
}

func (f Filesystem) Fetch(fpath string) (<-chan ReadCloserTagger, error) {
	fullpath := path.Join(f.Root, fpath)

	c := make(chan ReadCloserTagger)
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
			c <- File{o}
			return nil
		})
	}()

	return c, nil
}

type File struct {
	*os.File
}

func (f File) Tags() map[string]string {
	return nil
}
