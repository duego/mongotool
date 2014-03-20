package main

import (
	"fmt"
	"github.com/duego/mongotool/dump"
	"github.com/duego/mongotool/storage"
	"labix.org/v2/mgo"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var cmdDump = &Command{
	UsageLine: "dump -host [-collection] [-concurrency] [-target]",
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

The -concurrency flag specifies how many objects to dump to the target at the same time
`,
}

var (
	dumpHost        string // dump host flag
	dumpCollection  string // dump collection flag
	dumpTarget      string // dump target flag
	dumpProgress    bool   // dump progress flag
	dumpConcurrency int    // dump concurrency flag
)

func init() {
	cmdDump.Run = runDump
	cmdDump.Flag.StringVar(&dumpHost, "host", "localhost:27017/test", "")
	cmdDump.Flag.StringVar(&dumpCollection, "collection", "", "")
	cmdDump.Flag.StringVar(&dumpTarget, "target", "https://mongotool.s3.amazonaws.com/dump", "")
	cmdDump.Flag.BoolVar(&dumpProgress, "progress", true, "")
	cmdDump.Flag.IntVar(&dumpConcurrency, "concurrency", 1, "")
}

func worker(objects chan *dump.Object, errors chan error, store storage.Saver, root string) {
	for o := range objects {
		w, err := store.Save(
			strings.Join([]string{root, o.Database, o.Collection, o.Id.Hex()}, "/"),
		)
		if err == nil {
			_, err = w.Write(o.Bson)
			if err == nil {
				err = w.Close()
			}
		}
		errors <- err
	}
}

func runDump(cmd *Command, args []string) {
	fmt.Println("Connecting to", dumpHost)
	s, err := mgo.Dial(dumpHost + "?connect=direct")
	if err != nil {
		errorf("Error connecting to %s: %v", dumpHost, err)
		exit()
	}

	var (
		store storage.Saver
		root  string
	)
	// Figure out what kind of storage we're looking for
	if dumpTarget == "-" {
		errorf("%s", "TODO: Set stdout storage here")
		exit()
	}
	if dumpTarget[:4] == "http" {
		if u, err := url.Parse(dumpTarget); err != nil {
			errorf("%v", err)
			exit()
		} else {
			store = storage.S3{fmt.Sprintf("%s://%s", u.Scheme, u.Host)}
			root = u.Path
		}
	} else {
		store = storage.Filesystem{dumpTarget}
	}

	// Buffer additional objects exceeding one worker
	objects := make(chan *dump.Object, dumpConcurrency-1)

	// Errors from workers and final sync for any pending work
	errc := make(chan error, 1)

	for n := 0; n < dumpConcurrency; n++ {
		go worker(objects, errc, store, root)
	}

	fmt.Fprintln(os.Stderr, "Dumping")
	var (
		total   int64
		pending int64
	)

	count := make(chan int64)
	go func() {
		for o := range dump.Remote(s, dumpCollection) {
			objects <- o
			count <- 1
		}
		close(count)
		close(objects)
	}()

	formatString := fmt.Sprintf("\rObjects: %%d (pending saves: %%.%dd)", len(strconv.Itoa(dumpConcurrency)))
	for {
		select {
		case added, ok := <-count:
			// In case all work is sent, stop counting and wait for pending to finish
			if !ok {
				count = nil
				break
			}
			total += added
			pending += added
		case err := <-errc:
			if err != nil {
				errorf("\nError sending object to S3: %v", err)
				break
			}
			pending--
		}
		if dumpProgress {
			fmt.Fprintf(os.Stderr, formatString, total, pending)
		}

		// All objects have been sent and nothing is still pending, our work here is done!
		if count == nil && pending == 0 {
			break
		}
	}
	fmt.Fprintln(os.Stderr)
}
