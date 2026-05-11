package wal

import (
	"encoding/binary"
	"encoding/json"
)

// CheckpointRecord stores the Dirty Page Table and Active Transaction Table
// as a snapshot for ARIES recovery.
type CheckpointRecord struct {
	DPT       map[uint32]uint64 // pageID -> recLSN
	ATT       map[uint64]ATTEntry
	NextTxnID uint64
}

// ATTEntry is an entry in the Active Transaction Table.
type ATTEntry struct {
	Status      uint8 // TxnActive/TxnCommitted/TxnAborted
	LastLSN     uint64
	UndoNextLSN uint64
}

// EncodeCheckpoint serialises a CheckpointRecord to JSON bytes (simple, correct).
func EncodeCheckpoint(cr *CheckpointRecord) ([]byte, error) {
	return json.Marshal(cr)
}

// DecodeCheckpoint deserialises a CheckpointRecord.
func DecodeCheckpoint(data []byte) (*CheckpointRecord, error) {
	var cr CheckpointRecord
	if err := json.Unmarshal(data, &cr); err != nil {
		return nil, err
	}
	return &cr, nil
}

// WriteCheckpoint writes a LogCheckpoint record with the given DPT and ATT,
// then truncates WAL segments that are no longer needed for recovery.
func (w *WALManager) WriteCheckpoint(dpt map[uint32]uint64, att map[uint64]ATTEntry, nextTxnID uint64) (uint64, error) {
	cr := &CheckpointRecord{DPT: dpt, ATT: att, NextTxnID: nextTxnID}
	payload, err := EncodeCheckpoint(cr)
	if err != nil {
		return 0, err
	}
	lsn := w.AppendRecord(LogRecord{
		Type:    LogCheckpoint,
		Payload: payload,
	})
	if err := w.FlushUpTo(lsn); err != nil {
		return 0, err
	}

	// Truncate WAL segments that are entirely before min(checkpointLSN, minDPTRecLSN).
	// Any segment whose EndLSN <= truncateLSN is no longer needed for ARIES recovery.
	truncateLSN := lsn
	if minLSN := minDPTRecLSN(dpt); minLSN > 0 && minLSN < truncateLSN {
		truncateLSN = minLSN
	}
	w.TruncateUpTo(truncateLSN)

	return lsn, nil
}

// minDPTRecLSN returns the smallest recLSN in the dirty page table, or 0 if empty.
func minDPTRecLSN(dpt map[uint32]uint64) uint64 {
	var min uint64
	for _, lsn := range dpt {
		if min == 0 || lsn < min {
			min = lsn
		}
	}
	return min
}

// EncodeCLRPayload encodes the undoNextLSN for a Compensation Log Record.
func EncodeCLRPayload(undoNextLSN uint64, originalType LogRecordType, undonePayload []byte) []byte {
	buf := make([]byte, 8+1+len(undonePayload))
	binary.LittleEndian.PutUint64(buf[0:], undoNextLSN)
	buf[8] = byte(originalType)
	copy(buf[9:], undonePayload)
	return buf
}

// DecodeCLRPayload extracts the undoNextLSN, original record type, and original payload.
func DecodeCLRPayload(payload []byte) (uint64, LogRecordType, []byte) {
	if len(payload) < 8 {
		return 0, 0, nil
	}
	undoNext := binary.LittleEndian.Uint64(payload[0:])
	if len(payload) < 9 {
		return undoNext, 0, nil
	}
	return undoNext, LogRecordType(payload[8]), payload[9:]
}

// DecodeCLRUndoNext extracts the undoNextLSN from a CLR payload.
func DecodeCLRUndoNext(payload []byte) uint64 {
	undoNext, _, _ := DecodeCLRPayload(payload)
	return undoNext
}
