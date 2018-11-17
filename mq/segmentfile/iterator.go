package segmentfile

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

const invalidOffset = -1

type Iterator struct {
	file      *File
	offset    int64
	endOffset int64
	wait      bool
	reader    *bufio.Reader
	closed    uint32
}

func (i *Iterator) Next() (data []byte, offset int64, err error) {
	if atomic.LoadUint32(&i.closed) == 1 {
		return nil, invalidOffset, io.EOF
	}

	defer func() {
		if err != nil {
			atomic.StoreUint32(&i.closed, 1)
		}
	}()

NEXT:
	messageOffset := i.offset

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

			return nil, invalidOffset, err
		} else if err != nil {
			return nil, invalidOffset, errors.Wrap(err, "peek failed")
		}

		return nil, invalidOffset, errors.New("bad length")
	}

	if _, err := i.reader.Discard(n); err != nil {
		return nil, invalidOffset, errors.Wrap(err, "discard failed")
	}

	message := make([]byte, messageLength) // TODO: buffer pooling
	if _, err = io.ReadFull(i.reader, message); err != nil {
		return nil, invalidOffset, errors.Wrap(err, "read failed")
	}

	i.offset += int64(n) + int64(messageLength)

	return message, messageOffset, nil
}

func (i *Iterator) nextWait() error {
	if i.endOffset >= i.file.maxSize {
		return io.EOF
	}

	i.file.mutex.Lock()
	defer i.file.mutex.Unlock()

	for i.file.offset == i.endOffset {
		i.file.cond.Wait()
		if atomic.LoadUint32(&i.closed) == 1 {
			return io.EOF
		}
	}

	i.reader.Reset(io.NewSectionReader(i.file.file, i.endOffset, i.file.offset-i.endOffset))
	i.endOffset = i.file.offset

	return nil
}

func (i *Iterator) Close() error {
	atomic.StoreUint32(&i.closed, 1)
	i.file.cond.Broadcast()
	return nil
}
