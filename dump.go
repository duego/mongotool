package main

import (
	"bytes"
	"fmt"
	"github.com/duego/mongotool/dump"
	"github.com/duego/mongotool/storage"
	"labix.org/v2/mgo"
	"net/url"
	"strings"
)

var cmdDump = &Command{
	UsageLine: "dump -host [-collection] target",
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
`,
}

var (
	dumpHost       string // dump host flag
	dumpCollection string // dump collection flag
	dumpTarget     string // dump target flag
)

func init() {
	cmdDump.Run = runDump
	cmdDump.Flag.StringVar(&dumpHost, "host", "localhost:27017/test", "")
	cmdDump.Flag.StringVar(&dumpCollection, "collection", "test", "")
	cmdDump.Flag.StringVar(&dumpTarget, "target", "https://mongotool.s3.amazonaws.com/dump/", "")
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
	if u, err := url.Parse(dumpTarget); err != nil {
		// Probably not an S3 url if parsing fails, try disk storage
		errorf("%s", "TODO: Set disk storage here")
		exit()
	} else {
		store = storage.S3{fmt.Sprintf("%s://%s", u.Scheme, u.Host)}
		root = u.Path
	}

	for o := range dump.Remote(s, dumpCollection) {
		fmt.Println(o.Id, len(o.Bson))

		err := store.Save(
			strings.Join([]string{root, o.Database, o.Collection, o.Id.Hex()}, "/"),
			bytes.NewReader(o.Bson),
			[][]string{
				[]string{"Optime", "TODO"},
			},
		)

		if err != nil {
			errorf("Error sending object to S3: %v", err)
			exit()
		}
	}
}
