// Package wal implements the Write-Ahead Log with LSN-tagged, CRC32-protected records.
package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// LogRecordType identifies the kind of WAL record.
type LogRecordType uint8

const (
	LogBegin      LogRecordType = 1  // transaction started
	LogCommit     LogRecordType = 2  // transaction committed
	LogAbort      LogRecordType = 3  // transaction aborted
	LogInsert     LogRecordType = 4  // tuple inserted
	LogDelete     LogRecordType = 5  // tuple deleted (Xmax set)
	LogUpdate     LogRecordType = 6  // tuple updated
	LogSplit      LogRecordType = 7  // page split
	LogMerge      LogRecordType = 8  // pages merged
	LogCLR        LogRecordType = 9  // Compensation Log Record (undo)
	LogCheckpoint LogRecordType = 10 // checkpoint: DPT + ATT
	LogPageAlloc  LogRecordType = 11 // page allocated
	LogPageFree   LogRecordType = 12 // page freed
	LogRootChange LogRecordType = 13 // B+Tree root page ID changed; payload=[newRootID:4]
)

// LogRecordFixedSize is the byte count of everything except the payload.
// Wire format: [LSN:8][PrevLSN:8][TxnID:8][PageID:4][Type:1][PayloadLen:4][CRC32:4] = 37 bytes
const LogRecordFixedSize = 8 + 8 + 8 + 4 + 1 + 4 + 4

// Sentinel errors returned by log record decoding.
var (
	ErrInvalidLogRecord = errors.New("invalid log record")
	ErrCRCMismatch      = errors.New("log record CRC32 mismatch")
)

// LogRecord is the in-memory representation of a single WAL entry.
type LogRecord struct {
	LSN     uint64
	PrevLSN uint64
	TxnID   uint64
	PageID  uint32
	Type    LogRecordType
	Payload []byte
}

// EncodeLogRecord serialises a LogRecord to its wire format.
func EncodeLogRecord(r *LogRecord) []byte {
	buf := make([]byte, LogRecordFixedSize+len(r.Payload))
	binary.LittleEndian.PutUint64(buf[0:], r.LSN)
	binary.LittleEndian.PutUint64(buf[8:], r.PrevLSN)
	binary.LittleEndian.PutUint64(buf[16:], r.TxnID)
	binary.LittleEndian.PutUint32(buf[24:], r.PageID)
	buf[28] = byte(r.Type)
	binary.LittleEndian.PutUint32(buf[29:], uint32(len(r.Payload)))
	copy(buf[33:], r.Payload)
	crc := crc32.ChecksumIEEE(buf[:33+len(r.Payload)])
	binary.LittleEndian.PutUint32(buf[33+len(r.Payload):], crc)
	return buf
}

// DecodeLogRecord parses a LogRecord from its wire format and verifies the CRC32.
func DecodeLogRecord(data []byte) (*LogRecord, error) {
	if len(data) < LogRecordFixedSize {
		return nil, ErrInvalidLogRecord
	}
	r := &LogRecord{}
	r.LSN = binary.LittleEndian.Uint64(data[0:])
	r.PrevLSN = binary.LittleEndian.Uint64(data[8:])
	r.TxnID = binary.LittleEndian.Uint64(data[16:])
	r.PageID = binary.LittleEndian.Uint32(data[24:])
	r.Type = LogRecordType(data[28])
	payloadLen := int(binary.LittleEndian.Uint32(data[29:]))
	if len(data) < LogRecordFixedSize+payloadLen {
		return nil, ErrInvalidLogRecord
	}
	r.Payload = make([]byte, payloadLen)
	copy(r.Payload, data[33:])
	storedCRC := binary.LittleEndian.Uint32(data[33+payloadLen:])
	computedCRC := crc32.ChecksumIEEE(data[:33+payloadLen])
	if storedCRC != computedCRC {
		return nil, ErrCRCMismatch
	}
	return r, nil
}

// EncodedSize returns the total byte count for a record with the given payload length.
func EncodedSize(payloadLen int) int {
	return LogRecordFixedSize + payloadLen
}
