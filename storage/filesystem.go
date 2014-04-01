package storage

import (
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
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

func (f Filesystem) Walk(p string, wfunc WalkFunc) error {
	fullpath := path.Join(f.Root, p)
	return filepath.Walk(fullpath, func(fpath string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		relative := strings.TrimPrefix(fpath, f.Root)
		return wfunc(strings.TrimLeft(relative, "/"), err)
	})
}

func (f Filesystem) Fetch(fpath string) (io.ReadCloser, error) {
	return os.Open(path.Join(f.Root, fpath))
}
