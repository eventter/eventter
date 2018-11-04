package segmentfile

import (
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"

	"eventter.io/mq/msgid"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

var (
	ErrFull     = errors.New("segment is full")
	ErrReadOnly = errors.New("segment is read only")
	Encoding    = binary.LittleEndian
)

const (
	dataFileSuffix  = ".seg"
	indexFileSuffix = ".ind"
)

type File struct {
	id            uint64
	path          string
	maxSize       int64
	readOnly      bool
	offset        int64
	lastMessageID msgid.ID
	file          *os.File
	mutex         sync.Mutex
}

func NewFile(id uint64, path string, filePerm os.FileMode, maxSize int64) (f *File, err error) {
	file, err := os.OpenFile(path+dataFileSuffix, os.O_CREATE|os.O_RDWR, filePerm)
	if err != nil {
		return nil, errors.Wrap(err, "open failed")
	}
	defer func() {
		if err != nil {
			file.Close()
		}
	}()

	_, err = os.Stat(path + indexFileSuffix)
	var (
		readOnly      bool
		lastMessageID msgid.ID
	)
	if os.IsNotExist(err) {
		readOnly = false
	} else {
		readOnly = true
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	offset := stat.Size()
	var i int64 = 0
	buf := make([]byte, 4+msgid.Size)
	for i < offset {
		if _, err := file.Seek(i, io.SeekStart); err != nil {
			return nil, err
		}

		_, err := io.ReadFull(file, buf)
		if err != nil {
			return nil, err
		}

		messageSize := Encoding.Uint32(buf[0:4])
		lastMessageID.FromBytes(buf[4:])
		i += 4 + int64(messageSize)
	}

	return &File{
		id:            id,
		path:          path,
		file:          file,
		maxSize:       maxSize,
		readOnly:      readOnly,
		offset:        offset,
		lastMessageID: lastMessageID,
	}, nil
}

func (f *File) Write(id msgid.ID, message proto.Message) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.readOnly {
		return ErrReadOnly
	}

	messageBuf, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	// FIXME: checksum?
	buf := make([]byte, 4+msgid.Size+len(messageBuf))
	Encoding.PutUint32(buf[0:4], uint32(msgid.Size+len(messageBuf)))
	copy(buf[4:4+msgid.Size], id.Bytes())
	copy(buf[4+msgid.Size:], messageBuf)

	currentOffset := f.offset
	if currentOffset > f.maxSize {
		return ErrFull
	}

	if _, err := f.file.WriteAt(buf, currentOffset); err != nil {
		return err
	}

	f.offset += int64(len(buf))
	f.lastMessageID = id

	return nil
}

func (f *File) Complete() (sha1Sum []byte, size int64, lastMessageID msgid.ID, err error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if _, err := f.file.Seek(0, io.SeekStart); err != nil {
		return nil, 0, msgid.ID{}, err
	}

	h := sha1.New()
	if _, err := io.Copy(h, f.file); err != nil {
		return nil, 0, msgid.ID{}, err
	}

	return h.Sum(nil), f.offset, f.lastMessageID, nil
}

func (f *File) String() string {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return fmt.Sprintf(
		"segment %d: path=%s maxSize=%d readOnly=%t offset=%d lastMessageID=%s",
		f.id,
		f.path,
		f.maxSize,
		f.readOnly,
		f.offset,
		hex.EncodeToString(f.lastMessageID.Bytes()),
	)
}

func (f *File) Close() error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.file.Close()
}
