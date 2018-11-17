package segmentfile

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"os"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var ErrFull = errors.New("segment is full")

const (
	dataFileSuffix  = ".seg"
	indexFileSuffix = ".ind"
	version         = 1
)

type File struct {
	path    string
	maxSize int64
	offset  int64
	file    *os.File
	mutex   sync.Mutex
	cond    *sync.Cond

	// Following properties are used by Dir.

	id   uint64    // Segment ID.
	rc   int       // Reference count.
	idle time.Time // Time when reference count decreased to zero.
}

func Open(path string, filePerm os.FileMode, maxSize int64) (f *File, err error) {
	file, err := os.OpenFile(path+dataFileSuffix, os.O_CREATE|os.O_RDWR|os.O_APPEND|openSync, filePerm)
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
				return nil, errors.Errorf("wrong message size at offset %d", offset)
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
	}

	f.cond = sync.NewCond(&f.mutex)

	return f, nil
}

func (f *File) Write(message proto.Message) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	currentOffset := f.offset
	if currentOffset >= f.maxSize {
		return ErrFull
	}

	messageBuf, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	buf := make([]byte, binary.MaxVarintLen64+len(messageBuf)) // TODO: buffer pooling
	n := binary.PutUvarint(buf, uint64(len(messageBuf)))
	copy(buf[n:], messageBuf)
	buf = buf[:n+len(messageBuf)]

	if n, err := f.file.Write(buf); err != nil || n < len(buf) {
		if err := f.file.Truncate(f.offset); err != nil {
			panic("segment file " + f.path + " might be corrupted, truncate failed: " + err.Error())
		}
		return errors.Wrap(err, "write failed")
	}

	f.offset += int64(len(buf))

	f.cond.Broadcast()

	return nil
}

func (f *File) Size() int64 {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.offset
}

func (f *File) Read(wait bool) *Iterator {
	return f.ReadAt(1, wait)
}

func (f *File) ReadAt(offset int64, wait bool) *Iterator {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return &Iterator{
		file:   f,
		offset: f.offset,
		wait:   wait,
		reader: bufio.NewReader(io.NewSectionReader(f.file, offset, f.offset-offset)),
	}
}

func (f *File) Sum(h hash.Hash) (sum []byte, size int64, err error) {
	f.mutex.Lock()
	r := io.NewSectionReader(f.file, 0, f.offset)
	f.mutex.Unlock()

	if _, err := io.Copy(h, r); err != nil {
		return nil, 0, err
	}

	return h.Sum(nil), f.offset, nil
}

func (f *File) String() string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return fmt.Sprintf(
		"segment %d: path=%s maxSize=%d offset=%d",
		f.id,
		f.path,
		f.maxSize,
		f.offset,
	)
}

func (f *File) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Close()
}
