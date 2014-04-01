package mongo

import (
	"bytes"
	"encoding/json"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"strings"
)

const (
	objectIdKind = 0x07
	objectKind   = 0x03
)

// file Implements the storage.Filer interface
type File struct {
	*bytes.Reader
	name   string
	length int64
}

func NewFile(db, collection, name string, data []byte) *File {
	return &File{
		bytes.NewReader(data),
		strings.Join([]string{db, collection, name}, "/"),
		int64(len(data)),
	}
}

func (f *File) Path() string {
	return f.name
}

func (f *File) Length() int64 {
	return f.length
}

// Object represents one MongoDB object with attached metadata
type Object struct {
	// The raw bson object
	Bson []byte
	// The Object Id extracted from the raw bson
	Id bson.ObjectId
	// Collection is from what collection the object was fetched from
	Collection string
	// Database where the collection was read from
	Database string
}

func NewObject(db, col string) *Object {
	return &Object{
		// Use same initial buffer size as mgo.bson does.
		Bson:       make([]byte, 0, 64),
		Database:   db,
		Collection: col,
	}
}

// SetBSON implements the bson.Setter to let us only unmarshal the ObjectId while keeping the raw bytes
// as it was without having to unmarshal and then marshal everything to get them again.
func (o *Object) SetBSON(raw bson.Raw) error {
	o.Bson = append(o.Bson, raw.Data...)
	unmarshalled := struct {
		Id bson.ObjectId `bson:"_id"`
	}{}
	raw.Unmarshal(&unmarshalled)
	o.Id = unmarshalled.Id
	return nil
}

func (o *Object) GetBSON() (interface{}, error) {
	return bson.Raw{objectKind, o.Bson}, nil
}

// Dump will stream all objects from a collection on the returned channel
func Dump(s *mgo.Session, collection string) <-chan *File {
	c := make(chan *File)
	go func() {
		defer close(c)
		if s == nil {
			log.Println("No session")
			return
		}
		// Get the database selected by the connection string
		db := s.DB("")

		var collections []string
		if collection == "" {
			if cols, err := db.CollectionNames(); err != nil {
				log.Println(err)
				return
			} else {
				collections = cols
			}
		} else {
			collections = append(collections, collection)
		}

		for _, collection := range collections {
			// Skip internal system collections
			if strings.HasPrefix(collection, "system.") {
				continue
			}
			col := db.C(collection)

			// Dump indexes
			indexes, err := col.Indexes()
			if err != nil {
				log.Println(err)
			} else {
				indexJs, err := json.Marshal(indexes)
				if err != nil {
					log.Println(err)
				} else {
					c <- NewFile(db.Name, collection, "indexes.json", indexJs)
				}
			}

			// Dump all objects
			iter := col.Find(nil).Iter()
			for {
				result := NewObject(db.Name, collection)
				if iter.Next(result) {
					c <- NewFile(
						result.Database,
						result.Collection,
						result.Id.Hex(),
						result.Bson,
					)
				} else {
					break
				}
			}

			if iter.Timeout() {
				log.Println("Cursor timed out")
			}
			if err := iter.Close(); err != nil {
				log.Println(err)
			}
		}
	}()

	return c
}
