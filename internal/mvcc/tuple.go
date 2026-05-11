// Package mvcc implements Multi-Version Concurrency Control: tuples, transactions,
// snapshots, and visibility rules.
package mvcc

import (
	"encoding/binary"
	"errors"
)

// TupleHeaderSize is the fixed byte count before Key+Value in an encoded MVCC tuple.
const TupleHeaderSize = 8 + 8 + 2 + 2 + 1 // Xmin + Xmax + KeyLen + ValLen + Flags = 21

// TupleFlags encodes per-tuple status bits.
type TupleFlags uint8

// TupleFlags bit values.
const (
	TupleDeleted TupleFlags = 1 << iota // 0x01
	TupleUpdated                         // 0x02
	TupleLocked                          // 0x04
)

// ErrInvalidTuple is returned when decoding a malformed tuple.
var ErrInvalidTuple = errors.New("invalid tuple encoding")

// MVCCTuple is the in-memory representation of a versioned key-value record.
//
// Wire format: [Xmin:8][Xmax:8][KeyLen:2][ValLen:2][Flags:1][Key][Value]
type MVCCTuple struct {
	Xmin  uint64
	Xmax  uint64
	Flags TupleFlags
	Key   []byte
	Value []byte
}

// Encode serialises the tuple to its wire format.
func (t *MVCCTuple) Encode() []byte {
	buf := make([]byte, TupleHeaderSize+len(t.Key)+len(t.Value))
	binary.LittleEndian.PutUint64(buf[0:], t.Xmin)
	binary.LittleEndian.PutUint64(buf[8:], t.Xmax)
	binary.LittleEndian.PutUint16(buf[16:], uint16(len(t.Key)))
	binary.LittleEndian.PutUint16(buf[18:], uint16(len(t.Value)))
	buf[20] = byte(t.Flags)
	copy(buf[21:], t.Key)
	copy(buf[21+len(t.Key):], t.Value)
	return buf
}

// DecodeTuple deserialises a tuple from its wire format.
func DecodeTuple(data []byte) (*MVCCTuple, error) {
	if len(data) < TupleHeaderSize {
		return nil, ErrInvalidTuple
	}
	t := &MVCCTuple{}
	t.Xmin = binary.LittleEndian.Uint64(data[0:])
	t.Xmax = binary.LittleEndian.Uint64(data[8:])
	keyLen := int(binary.LittleEndian.Uint16(data[16:]))
	valLen := int(binary.LittleEndian.Uint16(data[18:]))
	t.Flags = TupleFlags(data[20])
	if len(data) < TupleHeaderSize+keyLen+valLen {
		return nil, ErrInvalidTuple
	}
	t.Key = make([]byte, keyLen)
	t.Value = make([]byte, valLen)
	copy(t.Key, data[21:])
	copy(t.Value, data[21+keyLen:])
	return t, nil
}

// TupleKeyExtract is the keyExtract function for use with slotted page operations.
// It returns the key bytes of an encoded MVCC tuple.
func TupleKeyExtract(data []byte) []byte {
	if len(data) < TupleHeaderSize {
		return nil
	}
	keyLen := int(binary.LittleEndian.Uint16(data[16:]))
	if len(data) < TupleHeaderSize+keyLen {
		return nil
	}
	return data[21 : 21+keyLen]
}
