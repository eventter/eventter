package segmentfile

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
)

type Iterator struct {
	file   *File
	offset int64
	wait   bool
	reader *bufio.Reader
}

func (i *Iterator) Next(message proto.Message) error {
NEXT:
	buf, err := i.reader.Peek(binary.MaxVarintLen64)
	messageLength, n := binary.Uvarint(buf)
	if n == 0 {
		if err == io.EOF {
			if i.wait {
				err = i.nextWait()
				if err == nil {
					goto NEXT
				}
			}
			return err
		} else if err != nil {
			return errors.Wrap(err, "peek failed")
		}

		return errors.New("bad length")
	}

	if _, err := i.reader.Discard(n); err != nil {
		return errors.Wrap(err, "discard failed")
	}

	messageBuf := make([]byte, messageLength)
	if _, err = io.ReadFull(i.reader, messageBuf); err != nil {
		return errors.Wrap(err, "read failed")
	}

	if err := proto.Unmarshal(messageBuf, message); err != nil {
		return errors.Wrap(err, "unmarshal failed")
	}

	return nil
}

func (i *Iterator) nextWait() error {
	if i.offset >= i.file.maxSize {
		return io.EOF
	}

	i.file.mutex.Lock()
	defer i.file.mutex.Unlock()

	for i.file.offset == i.offset {
		i.file.cond.Wait()
	}

	i.reader.Reset(io.NewSectionReader(i.file.file, i.offset, i.file.offset-i.offset))
	i.offset = i.file.offset

	return nil
}
