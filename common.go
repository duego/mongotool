package main

import (
	"fmt"
	"github.com/duego/mongotool/storage"
	"labix.org/v2/mgo"
	"net/url"
	"os"
	"strings"
)

// mongoSession gives a session or dies trying.
func mongoSession(addr string) *mgo.Session {
	fmt.Fprintln(os.Stderr, "Connecting to", addr)
	s, err := mgo.Dial(addr + "?connect=direct")
	if err != nil {
		errorf("Error connecting to %s: %v", addr, err)
		exit()
	}
	return s
}

// selectStorage will figure out what kind of storage we're looking for in specified target.
func selectStorage(target string, compression bool) (root string, store storage.SaveFetcher) {
	if target == "-" {
		errorf("%s", "TODO: Set stdin storage here")
		exit()
	}
	if strings.HasPrefix(target, "http") {
		if u, err := url.Parse(target); err != nil {
			errorf("%v", err)
			exit()
		} else {
			store = storage.NewS3(fmt.Sprintf("%s://%s", u.Scheme, u.Host))
			root = u.Path
		}
	} else {
		store = storage.Filesystem{target}
		root = ""
	}

	// Apply compression
	if compression {
		store = storage.NewGzipSaveFetcher(store)
	}

	return
}
