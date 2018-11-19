package segmentfile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
)

const (
	SumAll      = -1
	TruncateAll = 0
	version     = 1
)

var (
	ErrFull   = errors.New("segment is full")
	ErrExtend = errors.New("truncate would extend segment")
)

type File struct {
	path    string
	maxSize int64
	offset  int64
	term    uint32
	file    *os.File
	mutex   sync.Mutex
	cond    *sync.Cond

	// Following properties are used by Dir.

	id   uint64    // Segment ID.
	rc   int       // Reference count.
	idle time.Time // Time when reference count decreased to zero.
}

func Open(path string, filePerm os.FileMode, maxSize int64) (f *File, err error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|openSync, filePerm)
	if err != nil {
		return nil, errors.Wrap(err, "open failed")
	}
	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	stat, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "stat failed")
	}

	offset := int64(1)
	size := stat.Size()

	if size == 0 {
		if _, err := file.Write([]byte{version}); err != nil {
			return nil, errors.Wrap(err, "write version failed")
		}

	} else {
		buf := make([]byte, binary.MaxVarintLen64)

		if n, err := file.Read(buf[:1]); err != nil {
			return nil, errors.Wrap(err, "read version failed")
		} else if n == 0 {
			return nil, errors.New("read version failed")
		}

		if buf[0] != version {
			return nil, errors.Errorf("bad version, expected: %d, got: %d", version, buf[0])
		}

		for offset < size {
			if _, err := file.Seek(offset, io.SeekStart); err != nil {
				return nil, errors.Wrap(err, "seek failed")
			}

			n, err := file.Read(buf)
			if err != nil {
				return nil, errors.Wrap(err, "read failed")
			}

			buf = buf[:n]

			messageSize, n := binary.Uvarint(buf)
			if n == 0 {
				return nil, errors.Errorf("wrong message size at endOffset %d", offset)
			}

			increment := int64(n) + int64(messageSize)

			if offset+increment > size {
				if err := file.Truncate(offset); err != nil {
					return nil, errors.Wrap(err, "truncate failed")
				}
				break
			}

			offset += increment
		}
	}

	f = &File{
		path:    path,
		file:    file,
		maxSize: maxSize,
		offset:  offset,
		term:    1,
	}

	f.cond = sync.NewCond(&f.mutex)

	return f, nil
}

func (f *File) Write(message []byte) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	currentOffset := atomic.LoadInt64(&f.offset)
	if currentOffset >= f.maxSize {
		return ErrFull
	}

	buf := make([]byte, binary.MaxVarintLen64+len(message)) // TODO: buffer pooling
	n := binary.PutUvarint(buf, uint64(len(message)))
	copy(buf[n:], message)
	buf = buf[:n+len(message)]

	if n, err := f.file.Write(buf); err != nil || n < len(buf) {
		if err := f.file.Truncate(currentOffset); err != nil {
			panic("segment file " + f.path + " might be corrupted, truncate failed: " + err.Error())
		}
		return errors.Wrap(err, "write failed")
	}

	atomic.AddInt64(&f.offset, int64(len(buf)))

	f.cond.Broadcast()

	return nil
}

func (f *File) IsFull() bool {
	return atomic.LoadInt64(&f.offset) >= f.maxSize
}

func (f *File) Truncate(size int64) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if size < 0 {
		return errors.New("size must not be negative")
	} else if size == TruncateAll {
		size = 1 // version byte
	}

	currentOffset := atomic.LoadInt64(&f.offset)
	if size > currentOffset {
		return ErrExtend
	} else if size < currentOffset {
		if err := f.file.Truncate(size); err != nil {
			return errors.Wrap(err, "truncate failed")
		}
		atomic.StoreInt64(&f.offset, size)
	}

	atomic.AddUint32(&f.term, 1)

	f.cond.Broadcast()

	return nil
}

func (f *File) Read(wait bool) (*Iterator, error) {
	return f.ReadAt(1, wait)
}

func (f *File) ReadAt(offset int64, wait bool) (*Iterator, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if offset < 1 {
		return nil, errors.New("offset must be positive")
	}

	currentOffset := atomic.LoadInt64(&f.offset)
	if offset > currentOffset {
		return nil, errors.New("offset out of bounds")
	}

	return &Iterator{
		file:      f,
		term:      atomic.LoadUint32(&f.term),
		offset:    offset,
		endOffset: currentOffset,
		wait:      wait,
		reader:    bufio.NewReader(io.NewSectionReader(f.file, offset, currentOffset-offset)),
	}, nil
}

func (f *File) Sum(h hash.Hash, size int64) (sum []byte, actualSize int64, err error) {
	if size < 1 {
		size = atomic.LoadInt64(&f.offset)
	}
	r := io.NewSectionReader(f.file, 0, size)

	if _, err := io.Copy(h, r); err != nil {
		return nil, 0, err
	}

	return h.Sum(nil), size, nil
}

func (f *File) String() string {
	return fmt.Sprintf(
		"segment %d: path=%s maxSize=%d size=%d",
		f.id,
		f.path,
		f.maxSize,
		atomic.LoadInt64(&f.offset),
	)
}

func (f *File) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	atomic.AddUint32(&f.term, 1)

	f.cond.Broadcast()

	return f.file.Close()
}
