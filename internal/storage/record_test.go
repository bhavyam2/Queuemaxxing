package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"strings"
	"testing"
)

func TestEncodeFrameLayout(t *testing.T) {
	rec := Ack{ID: "abc"}
	frame, err := encodeFrame(rec)
	if err != nil {
		t.Fatalf("encodeFrame: %v", err)
	}

	payload, _ := json.Marshal(rec)
	if got := binary.BigEndian.Uint32(frame[0:4]); got != uint32(len(payload)) {
		t.Fatalf("length prefix = %d, want %d", got, len(payload))
	}
	if RecordType(frame[4]) != TypeAck {
		t.Fatalf("type byte = %d, want %d", frame[4], TypeAck)
	}
	if !bytes.Equal(frame[5:5+len(payload)], payload) {
		t.Fatalf("payload bytes do not match")
	}
	if len(frame) != frameOverhead+len(payload) {
		t.Fatalf("frame length = %d, want %d", len(frame), frameOverhead+len(payload))
	}

	// The checksum must cover the length and type bytes, not only the payload.
	want := crc32.Checksum(frame[:5+len(payload)], crcTable)
	if got := binary.BigEndian.Uint32(frame[5+len(payload):]); got != want {
		t.Fatalf("crc = %08x, want %08x", got, want)
	}
	frame[0] ^= 0xff
	if crc32.Checksum(frame[:5+len(payload)], crcTable) == want {
		t.Fatal("corrupting the length prefix did not change the checksum")
	}
}

func TestFrameRoundTripAllTypes(t *testing.T) {
	records := []Record{
		Header{Name: "orders", Ordering: "fifo", CreatedAt: 1, NextSeq: 7, TotalEnqueued: 9, TotalAcked: 2},
		Enqueue{ID: "id-1", Seq: 4, Priority: -3, CreatedAt: 10, AvailableAt: 20, Body: json.RawMessage(`{"k":[1,2,3]}`)},
		Ack{ID: "id-1"},
		Nack{ID: "id-1", AvailableAt: 99},
	}
	for _, rec := range records {
		frame, err := encodeFrame(rec)
		if err != nil {
			t.Fatalf("encodeFrame(%T): %v", rec, err)
		}
		n := binary.BigEndian.Uint32(frame[0:4])
		got, err := decodePayload(RecordType(frame[4]), frame[5:5+n])
		if err != nil {
			t.Fatalf("decodePayload(%T): %v", rec, err)
		}
		if !recordsEqual(got, rec) {
			t.Fatalf("round trip of %T: got %+v, want %+v", rec, got, rec)
		}
	}
}

func recordsEqual(a, b Record) bool {
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	return a.recordType() == b.recordType() && bytes.Equal(ja, jb)
}

func TestEncodeFrameRejectsOversizedPayload(t *testing.T) {
	body, _ := json.Marshal(strings.Repeat("x", MaxRecordSize))
	_, err := encodeFrame(Enqueue{ID: "big", Body: body})
	if err == nil {
		t.Fatal("expected an error for a payload past MaxRecordSize")
	}
}

func TestEncodeFrameRejectsEmptyBody(t *testing.T) {
	if _, err := encodeFrame(Enqueue{ID: "x"}); err == nil {
		t.Fatal("expected an error for an enqueue with no body")
	}
}

func TestFileHeader(t *testing.T) {
	h := encodeFileHeader()
	if len(h) != fileHeaderSize {
		t.Fatalf("file header is %d bytes, want %d", len(h), fileHeaderSize)
	}
	if err := checkFileHeader(h); err != nil {
		t.Fatalf("checkFileHeader: %v", err)
	}
	bad := append([]byte(nil), h...)
	bad[0] = 'X'
	if err := checkFileHeader(bad); err == nil {
		t.Fatal("expected bad magic to be rejected")
	}
	wrongVersion := append([]byte(nil), h...)
	binary.BigEndian.PutUint32(wrongVersion[8:12], formatVersion+1)
	if err := checkFileHeader(wrongVersion); err == nil {
		t.Fatal("expected an unknown format version to be rejected")
	}
	if err := checkFileHeader(h[:4]); err == nil {
		t.Fatal("expected a short header to be rejected")
	}
}

func TestRecordTypeValidity(t *testing.T) {
	for _, tt := range []RecordType{TypeHeader, TypeEnqueue, TypeAck, TypeNack} {
		if !tt.valid() {
			t.Fatalf("%v should be valid", tt)
		}
	}
	for _, tt := range []RecordType{TypeInvalid, 5, 255} {
		if tt.valid() {
			t.Fatalf("%d should not be valid", tt)
		}
	}
}
