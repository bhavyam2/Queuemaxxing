package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
)

// MaxRecordSize bounds a single record's JSON payload. A frame's length prefix is checked
// against it before any buffer is allocated, so a corrupt length cannot induce a huge
// allocation during recovery.
const MaxRecordSize = 8 << 20

const (
	lengthSize     = 4
	typeSize       = 1
	crcSize        = 4
	frameOverhead  = lengthSize + typeSize + crcSize
	fileHeaderSize = 16
	formatVersion  = 1
)

var magic = [8]byte{'Q', 'M', 'X', 'L', 'O', 'G', 0, 0}

// Castagnoli rather than IEEE: hardware-accelerated on amd64 and arm64 in the standard
// library, and it detects more multi-bit error patterns.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

type RecordType uint8

// Type 0 is reserved invalid so that a zero-filled region, which is what a torn write
// often leaves behind, is rejected rather than parsed as a legitimate frame.
const (
	TypeInvalid RecordType = 0
	TypeHeader  RecordType = 1
	TypeEnqueue RecordType = 2
	TypeAck     RecordType = 3
	TypeNack    RecordType = 4
)

func (t RecordType) valid() bool {
	return t >= TypeHeader && t <= TypeNack
}

func (t RecordType) String() string {
	switch t {
	case TypeHeader:
		return "HEADER"
	case TypeEnqueue:
		return "ENQUEUE"
	case TypeAck:
		return "ACK"
	case TypeNack:
		return "NACK"
	default:
		return fmt.Sprintf("INVALID(%d)", uint8(t))
	}
}

// Record is the sum type written to the log. All timestamps are Unix milliseconds:
// a monotonic clock reading cannot be serialized, so persisted times are wall clock.
type Record interface {
	recordType() RecordType
}

// Header is the first record of every log. It carries the queue configuration plus the
// counters and sequence floor that compaction would otherwise reset, since a compacted
// log no longer contains the ENQUEUE and ACK frames those values were derived from.
type Header struct {
	Name          string `json:"name"`
	Ordering      string `json:"ordering"`
	CreatedAt     int64  `json:"created_at"`
	NextSeq       uint64 `json:"next_seq"`
	TotalEnqueued uint64 `json:"total_enqueued"`
	TotalAcked    uint64 `json:"total_acked"`
}

type Enqueue struct {
	ID          string          `json:"id"`
	Seq         uint64          `json:"seq"`
	Priority    int             `json:"priority"`
	CreatedAt   int64           `json:"created_at"`
	AvailableAt int64           `json:"available_at"`
	Body        json.RawMessage `json:"body"`
}

type Ack struct {
	ID string `json:"id"`
}

type Nack struct {
	ID          string `json:"id"`
	AvailableAt int64  `json:"available_at"`
}

func (Header) recordType() RecordType  { return TypeHeader }
func (Enqueue) recordType() RecordType { return TypeEnqueue }
func (Ack) recordType() RecordType     { return TypeAck }
func (Nack) recordType() RecordType    { return TypeNack }

// encodeFrame lays out [uint32 payload length][uint8 type][payload][uint32 CRC-32C].
// The checksum covers the length and type bytes as well as the payload, so a corrupted
// length is detectable instead of silently steering the reader to the wrong offset.
func encodeFrame(rec Record) ([]byte, error) {
	if e, ok := rec.(Enqueue); ok && len(e.Body) == 0 {
		return nil, fmt.Errorf("enqueue record %s has an empty body", e.ID)
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal %s record: %w", rec.recordType(), err)
	}
	if len(payload) == 0 {
		return nil, fmt.Errorf("marshal %s record: empty payload", rec.recordType())
	}
	if len(payload) > MaxRecordSize {
		return nil, fmt.Errorf("%s record payload is %d bytes, limit is %d", rec.recordType(), len(payload), MaxRecordSize)
	}

	frame := make([]byte, frameOverhead+len(payload))
	binary.BigEndian.PutUint32(frame[0:lengthSize], uint32(len(payload)))
	frame[lengthSize] = byte(rec.recordType())
	copy(frame[lengthSize+typeSize:], payload)
	crc := crc32.Checksum(frame[:lengthSize+typeSize+len(payload)], crcTable)
	binary.BigEndian.PutUint32(frame[lengthSize+typeSize+len(payload):], crc)
	return frame, nil
}

func decodePayload(t RecordType, payload []byte) (Record, error) {
	switch t {
	case TypeHeader:
		var r Header
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		return r, nil
	case TypeEnqueue:
		var r Enqueue
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		return r, nil
	case TypeAck:
		var r Ack
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		return r, nil
	case TypeNack:
		var r Nack
		if err := json.Unmarshal(payload, &r); err != nil {
			return nil, err
		}
		return r, nil
	default:
		return nil, fmt.Errorf("unknown record type %d", uint8(t))
	}
}

func encodeFileHeader() []byte {
	b := make([]byte, fileHeaderSize)
	copy(b[0:8], magic[:])
	binary.BigEndian.PutUint32(b[8:12], formatVersion)
	return b
}

func checkFileHeader(b []byte) error {
	if len(b) < fileHeaderSize {
		return fmt.Errorf("file header is %d bytes, want %d", len(b), fileHeaderSize)
	}
	if string(b[0:8]) != string(magic[:]) {
		return fmt.Errorf("bad magic %q, not a queuemaxxing log", b[0:8])
	}
	if v := binary.BigEndian.Uint32(b[8:12]); v != formatVersion {
		return fmt.Errorf("log format version %d, this build understands %d", v, formatVersion)
	}
	return nil
}
