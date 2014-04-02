package main

import (
	"archive/tar"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/duego/mongotool/mongo"
	"github.com/duego/mongotool/storage"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"os"
	"strings"
)

var cmdRestore = &Command{
	UsageLine: "restore [-host address] [-source path]",
	Short:     "restore database from S3 bucket, filesystem or stdin",
	Long: `
Restore reads objects from a bucket on Amazon S3, filesystem or standard input.
The objects are written to collections of the specified database.
For the authentication towards S3 to work, you need to set the environment
variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

The -host flag specifies which host and database to write to.
For example to select "test" database of localhost: localhost:27017/test

The -source flag specifies which of S3 bucket, filesystem or stdout to read from.

S3 bucket is recognized when target path is in the form: "https://mongotool.s3.amazonaws.com/test".
This would use the mongotool bucket with "test" as its root.

Filesystem is used when a url is not recognized.

Finally stdin is used if "-" is specified.

Set -compression to false if the dump did not have compression enabled.

Set -indexes to false to skip ensure indexes.
`,
}

var (
	// restore flags
	restoreHost       string
	restoreSource     string
	restoreProgress   bool
	restoreCompressed bool
	restoreIndexes    bool
)

func init() {
	cmdRestore.Run = runRestore
	cmdRestore.Flag.StringVar(&restoreHost, "host", "localhost:27017/test", "")
	cmdRestore.Flag.StringVar(&restoreSource, "source", "https://mongotool.s3.amazonaws.com/dump", "")
	cmdRestore.Flag.BoolVar(&restoreProgress, "progress", true, "")
	cmdRestore.Flag.BoolVar(&restoreCompressed, "compression", true, "")
	cmdRestore.Flag.BoolVar(&restoreIndexes, "indexes", true, "")
}

// entryToObject constructs a mongo object from the tar entry
func entryToObject(name string, r io.Reader) (o *mongo.Object, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 {
		return o, errors.New("Expected db, col and id in header")
	}
	if len(parts[2]) != 24 {
		return o, errors.New("Invalid object id: " + name)
	}
	db, col, id := parts[0], parts[1], bson.ObjectIdHex(parts[2])
	o = mongo.NewObject(db, col)
	o.Id = id
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}
	o.Bson = b
	return
}

// entryToIndexes returns a mongo index from the tar entry
func entryToIndexes(name string, r io.Reader) (col string, index []*mgo.Index, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 {
		err = errors.New("Expected db, col and indexes.json in header")
		return
	}
	col = parts[1]

	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}
	index = make([]*mgo.Index, 0)
	err = json.Unmarshal(b, &index)
	return
}

func runRestore(cmd *Command, args []string) {
	root, store := selectStorage(restoreSource, restoreCompressed)
	db := mongoSession(restoreHost).DB("")

	var total int64
	colIndexes := make(map[string][]*mgo.Index, 0)
	err := store.(storage.Walker).Walk(root, func(fpath string, err error) error {
		if err != nil {
			return err
		}
		r, err := store.Fetch(fpath)
		if err != nil {
			return err
		}
		tr := tar.NewReader(r)
		for {
			h, err := tr.Next()
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if !strings.HasSuffix(h.Name, "/indexes.json") {
				o, err := entryToObject(h.Name, tr)
				if err != nil {
					return err
				}
				err = db.C(o.Collection).Insert(o)
				if err != nil {
					return err
				}
				if restoreProgress {
					total++
					fmt.Fprintf(os.Stderr, "\rObjects: %d", total)
				}
			} else if restoreIndexes {
				// Save indexes to be applied as a last step.
				col, indexes, err := entryToIndexes(h.Name, tr)
				if err != nil {
					return err
				}
				if _, ok := colIndexes[col]; ok {
					return errors.New("Indexes was already stored for: " + col)
				}
				colIndexes[col] = indexes
			}
		}
	})
	fmt.Fprintln(os.Stderr)

indexes:
	for col, indexes := range colIndexes {
		fmt.Fprintln(os.Stderr, "Applying indexes for", col)
		for _, index := range indexes {
			err := db.C(col).EnsureIndex(*index)
			if err != nil {
				break indexes
			}
		}
	}
	if err != nil {
		errorf("%v", err)
		exit()
	}
}
