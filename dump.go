package main

import (
	"archive/tar"
	"fmt"
	"github.com/duego/mongotool/mongo"
	"github.com/duego/mongotool/storage"
	"io"
	"math/rand"
	"os"
	"path"
	"strings"
	"time"
)

var cmdDump = &Command{
	UsageLine: "dump [-host address] [-collection name] [-concurrency num] [-target path]",
	Short:     "dump database to S3 bucket, filesystem or stdout",
	Long: `
Dump reads one or all collections of the specified database and
stores the objects to a bucket on Amazon S3, filesystem path or standard output.
For the authentication towards S3 to work, you need to set the environment
variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

The -host flag specifies which host and database to read from.
For example to select "test" database of localhost: localhost:27017/test

The -collection flag causes dump to only read from one collection of
the specified database, instead of all collections found.

The -target flag specifies which of S3 bucket, filesystem or stdout to write to.

S3 bucket is recognized when target path is in the form: "https://mongotool.s3.amazonaws.com/test".
This would use the mongotool bucket with "test" as its root.

Filesystem is used when a url is not recognized.

Finally stdout is used if "-" is specified.

Set -size to pick how many MB of bson we should read until moving on with the next chunk of data.

The -compress flag specifies if we should compress data before hitting the target storage.

The -concurrency flag specifies how many objects to dump to the target at the same time

If the -progress flag is set to true, an object count will be displayed
`,
}

var (
	dumpHost        string // dump host flag
	dumpCollection  string // dump collection flag
	dumpTarget      string // dump target flag
	dumpProgress    bool   // dump progress flag
	dumpConcurrency int    // dump concurrency flag
	dumpSize        int    // dump size flag
	dumpCompress    bool   // dump compression flag
)

func init() {
	cmdDump.Run = runDump
	cmdDump.Flag.StringVar(&dumpHost, "host", "localhost:27017/test", "")
	cmdDump.Flag.StringVar(&dumpCollection, "collection", "", "")
	cmdDump.Flag.StringVar(&dumpTarget, "target", "https://mongotool.s3.amazonaws.com/dump", "")
	cmdDump.Flag.IntVar(&dumpSize, "size", 1000, "Megabytes per stored chunk")
	cmdDump.Flag.BoolVar(&dumpProgress, "progress", true, "")
	cmdDump.Flag.BoolVar(&dumpCompress, "compression", true, "")
	cmdDump.Flag.IntVar(&dumpConcurrency, "concurrency", 1, "")
}

func randString(length int) string {
	alpha := "abcdefghijklmnopqrstuvxyzABCDEFGHIJKLMNOPQRSTUVXYZ"
	s := ""
	for n := 0; n < length; n++ {
		s += string(alpha[rand.Intn(length)])
	}
	return s
}

// Worker is responsible of writing the tar archive to storage.
// The amount of object data read into each file is contrained to specified size.
func worker(objects chan storage.Filer, errors chan error, store storage.Saver, root string, size int) {
chunk:
	for {
		// New chunk of data for specified size
		remaining := storage.ByteSize(size) * storage.MB
		w, err := store.Save(path.Join(root, "dump."+randString(5)))
		if err != nil {
			errorf("Could not open writer: %v", err)
			exit()
		}
		// Read objects into chunk
		for o := range objects {
			// New file entry in tar archive
			tw := tar.NewWriter(w)
			if err := tw.WriteHeader(&tar.Header{
				Name:     o.Path(),
				Mode:     0644,
				Size:     o.Length(),
				ModTime:  time.Now(),
				Typeflag: tar.TypeReg,
				Uid:      os.Getuid(),
				Gid:      os.Getegid(),
			}); err != nil {
				errors <- err
				continue
			}
			if n, err := io.Copy(tw, o); err != nil {
				errors <- err
				continue
			} else {
				remaining -= storage.ByteSize(n)
			}
			// Since Close would write the end sequence of tar archive, we only flush it.
			if err := tw.Flush(); err != nil {
				errors <- err
				continue
			}
			// If we have read all of the allowed size, move on to the next chunk.
			if remaining <= 0 {
				errors <- w.Close()
				continue chunk
			}
		}
		errors <- w.Close()
		return
	}
}

func runDump(cmd *Command, args []string) {
	root, store := selectStorage(dumpTarget, dumpCompress)

	// Buffer additional objects exceeding one worker
	objects := make(chan storage.Filer, dumpConcurrency-1)

	// Errors from workers and final sync for any pending work
	errc := make(chan error, 1)

	done := make(chan bool)
	for n := 0; n < dumpConcurrency; n++ {
		go func() {
			worker(objects, errc, store, root, dumpSize)
			done <- true
		}()
	}

	count := make(chan bool)
	go func() {
		for o := range mongo.Dump(mongoSession(dumpHost), dumpCollection) {
			objects <- o
			// Don't count indexes as "objects"
			if !strings.HasSuffix(o.Path(), "/indexes.json") {
				count <- true
			}
		}
		close(count)
		close(objects)
	}()

	fmt.Fprintln(os.Stderr, "Dumping")
	var total int64
	pending := dumpConcurrency
	for {
		select {
		case _, ok := <-count:
			// In case all work is sent, stop counting and wait for pending to finish
			if !ok {
				count = nil
				break
			}
			total++
		case err := <-errc:
			if err != nil {
				errorf("\nError saving object: %v", err)
				break
			}
		case <-done:
			pending--
		}
		if dumpProgress {
			fmt.Fprintf(os.Stderr, "\rObjects: %d", total)
		}

		// All objects have been sent and nothing is still pending, our work here is done!
		if count == nil && pending == 0 {
			break
		}
	}
	fmt.Fprintln(os.Stderr)
}
