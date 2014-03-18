package bson

import (
	"encoding/binary"
	"io"
	"labix.org/v2/mgo/bson"
)

// BSON documents are little endian
var Pack = binary.LittleEndian

// UnmarshalFromStream was borrowed from Youtube vitess package, but modified to use
// mgo bson unmarshal as the types for mongo are a bit different.
func UnmarshalFromStream(reader io.Reader, out interface{}) (err error) {
	lenbuf := make([]byte, 4)
	var n int
	n, err = io.ReadFull(reader, lenbuf)
	if err != nil {
		return err
	}
	if n != 4 {
		return io.ErrUnexpectedEOF
	}
	length := Pack.Uint32(lenbuf)
	b := make([]byte, length)
	Pack.PutUint32(b, length)
	n, err = io.ReadFull(reader, b[4:])
	if err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}
	if n != int(length-4) {
		return io.ErrUnexpectedEOF
	}
	if out == nil {
		return nil
	}
	return bson.Unmarshal(b, out)
}

func MarshalToStream(writer io.Writer, val interface{}) (err error) {
	out, err := bson.Marshal(val)
	if err != nil {
		return err
	}
	_, err = writer.Write(out)
	return err
}
