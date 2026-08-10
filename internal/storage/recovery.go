package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log"
	"os"
	"path/filepath"
)

// ErrIncompleteLog reports a log whose 16-byte file header was never fully written, which
// means the queue's creation was interrupted before it was acknowledged to any client.
var ErrIncompleteLog = errors.New("log file header is incomplete")

// CorruptError reports damage that is not at the tail of the log. Records after the damage
// were acknowledged to clients, so recovery refuses to continue rather than skip them.
type CorruptError struct {
	Path   string
	Offset int64
	Reason string
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("%s: corrupt record at byte offset %d: %s", e.Path, e.Offset, e.Reason)
}

// tornTail reports damage that reaches the end of the file. The frame it describes belongs
// to an operation no client ever received a 2xx for, so discarding it is correct.
type tornTail struct {
	offset int64
	reason string
}

func (t *tornTail) Error() string {
	return fmt.Sprintf("torn frame at byte offset %d: %s", t.offset, t.reason)
}

// scanner walks frames sequentially. Damage is classified as a torn tail or as corruption
// by whether the damaged frame reaches the end of the file:
//
//   - running out of bytes part way through a frame is always a torn tail;
//   - a complete but unverifiable frame is a torn tail only when nothing follows it,
//     since a partial write can leave a full-length region with stale or zeroed bytes;
//   - anything else has valid data after it and is reported as corruption.
type scanner struct {
	r      *bufio.Reader
	offset int64
	size   int64
}

func (s *scanner) next() (RecordType, []byte, error) {
	var lenBuf [lengthSize]byte
	n, err := io.ReadFull(s.r, lenBuf[:])
	if err == io.EOF {
		return TypeInvalid, nil, io.EOF
	}
	if err != nil {
		return TypeInvalid, nil, &tornTail{s.offset, fmt.Sprintf("length prefix truncated after %d bytes", n)}
	}

	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	remaining := s.size - s.offset
	if payloadLen < 1 || payloadLen > MaxRecordSize {
		// A length outside the valid range is undecodable, so there is no frame end to
		// compare against EOF. A zero length means the four length bytes were themselves
		// zero: if the rest of the file is zeros too, this is the zero fill a partial
		// write leaves behind, and nothing beyond it was ever committed. Non-zero garbage
		// is not explainable that way and is reported as corruption.
		if payloadLen == 0 && restIsZero(s.r) {
			return TypeInvalid, nil, &tornTail{s.offset, fmt.Sprintf("zero fill for the final %d bytes", remaining)}
		}
		if remaining < frameOverhead+1 {
			return TypeInvalid, nil, &tornTail{s.offset, fmt.Sprintf("length %d with only %d bytes left in file", payloadLen, remaining)}
		}
		return TypeInvalid, nil, &CorruptError{Offset: s.offset, Reason: fmt.Sprintf("length %d is outside [1, %d]", payloadLen, MaxRecordSize)}
	}

	frameSize := int64(frameOverhead) + int64(payloadLen)
	if s.offset+frameSize > s.size {
		return TypeInvalid, nil, &tornTail{s.offset, fmt.Sprintf("frame of %d bytes runs past end of file", frameSize)}
	}

	rest := make([]byte, typeSize+int(payloadLen)+crcSize)
	if _, err := io.ReadFull(s.r, rest); err != nil {
		return TypeInvalid, nil, &tornTail{s.offset, "frame body truncated"}
	}

	recType := RecordType(rest[0])
	payload := rest[typeSize : typeSize+int(payloadLen)]
	want := binary.BigEndian.Uint32(rest[typeSize+int(payloadLen):])

	crc := crc32.Update(crc32.Update(0, crcTable, lenBuf[:]), crcTable, rest[:typeSize+int(payloadLen)])
	atEOF := s.offset+frameSize == s.size

	if crc != want {
		reason := fmt.Sprintf("checksum mismatch (have %08x, want %08x)", crc, want)
		if atEOF {
			return TypeInvalid, nil, &tornTail{s.offset, reason}
		}
		return TypeInvalid, nil, &CorruptError{Offset: s.offset, Reason: reason}
	}
	if !recType.valid() {
		reason := fmt.Sprintf("record type %d is not valid", uint8(recType))
		if atEOF {
			return TypeInvalid, nil, &tornTail{s.offset, reason}
		}
		return TypeInvalid, nil, &CorruptError{Offset: s.offset, Reason: reason}
	}

	s.offset += frameSize
	return recType, payload, nil
}

// restIsZero reports whether every remaining byte is zero. It consumes the reader, which is
// safe because the scan is already being abandoned by the time it is called.
func restIsZero(r *bufio.Reader) bool {
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		for _, b := range buf[:n] {
			if b != 0 {
				return false
			}
		}
		if err == io.EOF {
			return true
		}
		if err != nil {
			return false
		}
	}
}

// Open replays an existing log, handing every record to apply in write order, and returns a
// log positioned to append. A torn tail is truncated away; corruption anywhere else fails.
func Open(dir, name string, apply func(Record) error) (*Log, error) {
	path := filepath.Join(dir, name+logExt)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			f.Close()
		}
	}()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < fileHeaderSize {
		return nil, fmt.Errorf("%s: %w (%d bytes)", path, ErrIncompleteLog, size)
	}

	hdr := make([]byte, fileHeaderSize)
	if _, err := io.ReadFull(f, hdr); err != nil {
		return nil, fmt.Errorf("%s: read file header: %w", path, err)
	}
	if err := checkFileHeader(hdr); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	s := &scanner{r: bufio.NewReaderSize(f, 64<<10), offset: fileHeaderSize, size: size}
	records := 0
	for {
		frameStart := s.offset
		recType, payload, err := s.next()
		if err == io.EOF {
			break
		}
		var torn *tornTail
		if errors.As(err, &torn) {
			log.Printf("queuemaxxing: %s: %v; truncating to %d bytes", path, torn, torn.offset)
			if err := truncate(f, dir, torn.offset); err != nil {
				return nil, fmt.Errorf("%s: truncate torn tail: %w", path, err)
			}
			size = torn.offset
			break
		}
		var corrupt *CorruptError
		if errors.As(err, &corrupt) {
			corrupt.Path = path
			return nil, corrupt
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}

		rec, err := decodePayload(recType, payload)
		if err != nil {
			// The checksum already passed, so the bytes are exactly what was written.
			// Undecodable content is a format bug, not media damage, and skipping it
			// would drop an acknowledged operation.
			return nil, &CorruptError{Path: path, Offset: frameStart, Reason: fmt.Sprintf("decode %s payload: %v", recType, err)}
		}
		if records == 0 && recType != TypeHeader {
			return nil, &CorruptError{Path: path, Offset: frameStart, Reason: fmt.Sprintf("first record is %s, want HEADER", recType)}
		}
		if err := apply(rec); err != nil {
			return nil, fmt.Errorf("%s: apply %s record: %w", path, recType, err)
		}
		records++
	}

	if records == 0 {
		return nil, fmt.Errorf("%s: %w (no header record)", path, ErrIncompleteLog)
	}

	ok = true
	return &Log{f: f, path: path, dir: dir, size: size, records: records}, nil
}

func truncate(f *os.File, dir string, at int64) error {
	if err := f.Truncate(at); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return syncDir(dir)
}
