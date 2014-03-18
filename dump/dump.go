package dump

import (
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

const objectIdKind = 0x07

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

// Remote will stream all objects from a collection on the returned channel
func Remote(s *mgo.Session, collection string) <-chan *Object {
	// TODO: Pass errors somewhere? Perhaps a method like iter.Close
	c := make(chan *Object)
	go func() {
		defer close(c)
		if s == nil {
			log.Println("No session")
			return
		}
		if collection == "" {
			// TODO: Dump all collections by default
			// cols, _ := db.CollectionNames()
			log.Println("No collection specified")
			return
		}

		db := s.DB("")
		iter := db.C(collection).Find(nil).Iter()

		for {
			result := NewObject(db.Name, collection)
			for iter.Next(result) {
				c <- result
			}
			break
		}

		if iter.Timeout() {
			log.Println("Cursor timed out")
		}
		if err := iter.Close(); err != nil {
			log.Println(err)
		}
	}()

	return c
}
