package main

import (
	"bytes"
	"fmt"
	"github.com/duego/mongotool/dump"
	"github.com/smartystreets/go-aws-auth"
	"io"
	"io/ioutil"
	"labix.org/v2/mgo"
	"net/http"
	"os"
	"strings"
)

var cmdDump = &Command{
	UsageLine: "dump -host [-collection] [-bucket] [-rootpath]",
	Short:     "dump database to S3 bucket",
	Long: `
Dump reads one or all collections of the specified database and
uploads the objects to a bucket on Amazon S3.
For the authentication towards S3 to work, you need to set the environment
variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

In the list, DIR represents the final path element of the
directory, and MAINFILE is the base name of any Go source
file in the directory that is not included when building
the package.

The -host flag specifies which host and database to read from.
For example to select "test" database of localhost: localhost:27017/test

The -collection flag causes dump to only read from one collection of
the specified database, instead of all collections found.

The -bucket flag specifies which S3 bucket to write to.

The -rootpath flag defined in what path of the bucket we should start populating
our dumped objects. Leave this empty to use the top level of the bucket.
`,
}

var (
	dumpHost       string // dump host flag
	dumpCollection string // dump collection flag
	dumpBucket     string // dump bucket flag
	dumpPath       string // dump rootpath flag
)

func init() {
	cmdDump.Run = runDump
	cmdDump.Flag.StringVar(&dumpHost, "host", "localhost:27017/test", "")
	cmdDump.Flag.StringVar(&dumpCollection, "collection", "test", "")
	cmdDump.Flag.StringVar(&dumpBucket, "bucket", "mongotool", "")
	cmdDump.Flag.StringVar(&dumpPath, "rootpath", "dump", "")
}

func S3Object(bucket, path string, body io.Reader, tags [][]string) (req *http.Request, err error) {
	if len(path) > 0 {
		if string(path[0]) != "/" {
			path = "/" + path
		}
	}
	if req, err = http.NewRequest("PUT", "https://"+bucket+".s3.amazonaws.com"+path, body); err != nil {
		return
	}
	for _, tag := range tags {
		key, value := tag[0], tag[1]
		req.Header.Add("x-amz-meta-"+key, value)
	}

	awsauth.SignS3(req)
	return
}

func checkAwsKeys() {
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" {
		errorf("%s", "Missing AWS_ACCESS_KEY_ID environment variable")
		exit()
	}
	if os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		errorf("%s", "Missing AWS_SECRET_ACCESS_KEY environment variable")
		exit()
	}
}

func runDump(cmd *Command, args []string) {
	checkAwsKeys()

	s, err := mgo.Dial(dumpHost + "?connect=direct")
	if err != nil {
		errorf("Error connecting to %s: %v", dumpHost, err)
		exit()
	}
	for o := range dump.Remote(s, dumpCollection) {
		fmt.Println(o.Id, len(o.Bson))

		s3req, err := S3Object(
			dumpBucket,
			strings.Join([]string{dumpPath, o.Database, o.Collection, o.Id.Hex()}, "/"),
			bytes.NewReader(o.Bson),
			[][]string{
				[]string{"Optime", "TODO"},
			},
		)
		if err != nil {
			errorf("%v", err)
			exit()
		}
		client := http.DefaultClient
		resp, err := client.Do(s3req)
		if err != nil {
			errorf("Error sending object to S3: %v", err)
			exit()
		}
		if code := resp.StatusCode; code != 200 {
			msg, _ := ioutil.ReadAll(resp.Body)
			resp.Body.Close()
			errorf("Exected http 200 code back, got: (%d) %s", code, string(msg))
			exit()
		}
	}
}
