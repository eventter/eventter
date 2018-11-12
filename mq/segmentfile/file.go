package segmentfile

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	ErrFull  = errors.New("segment is full")
	Encoding = binary.LittleEndian
)

const (
	dataFileSuffix  = ".seg"
	indexFileSuffix = ".ind"
)

type File struct {
	id      uint64
	path    string
	maxSize int64
	offset  int64
	file    *os.File
	mutex   sync.Mutex
}

func NewFile(id uint64, path string, filePerm os.FileMode, maxSize int64) (f *File, err error) {
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
		return nil, err
	}

	offset := stat.Size()
	var i int64 = 0
	buf := make([]byte, 4+4)
	for i < offset {
		if _, err := file.Seek(i, io.SeekStart); err != nil {
			return nil, err
		}

		_, err := io.ReadFull(file, buf)
		if err != nil {
			return nil, err
		}

		messageSize := Encoding.Uint32(buf[0:4])
		i += 4 + int64(messageSize)
	}

	return &File{
		id:      id,
		path:    path,
		file:    file,
		maxSize: maxSize,
		offset:  offset,
	}, nil
}

func (f *File) Write(message proto.Message) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	currentOffset := f.offset
	if currentOffset > f.maxSize {
		return ErrFull
	}

	messageBuf, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	buf := make([]byte, 4+4+len(messageBuf))
	Encoding.PutUint32(buf[0:4], uint32(4+len(messageBuf)))
	Encoding.PutUint32(buf[4:8], crc32.ChecksumIEEE(messageBuf))
	copy(buf[8:], messageBuf)

	if _, err := f.file.Write(buf); err != nil {
		return err
	}

	f.offset += int64(len(buf))

	return nil
}

func (f *File) Complete() (sha1Sum []byte, size int64, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}

	h := sha1.New()
	if _, err := io.Copy(h, f.file); err != nil {
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
