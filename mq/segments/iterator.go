package segments

import (
	"bufio"
	"encoding/binary"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

const invalidOffset = -1

var (
	ErrIteratorClosed  = errors.New("iterator is closed")
	ErrIteratorInvalid = errors.New("iterator is invalid (segment term changed)")
)

type Iterator struct {
	file      *File
	term      uint32
	offset    int64
	endOffset int64
	wait      bool
	reader    *bufio.Reader
	closed    uint32
}

func (i *Iterator) Next() (data []byte, offset int64, commitOffset int64, err error) {
	if atomic.LoadUint32(&i.closed) == 1 {
		return nil, invalidOffset, invalidOffset, ErrIteratorClosed
	}

	if atomic.LoadUint32(&i.file.term) != i.term {
		return nil, invalidOffset, invalidOffset, ErrIteratorInvalid
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

			return nil, invalidOffset, invalidOffset, err
		} else if err != nil {
			return nil, invalidOffset, invalidOffset, errors.Wrap(err, "peek failed")
		}

		return nil, invalidOffset, invalidOffset, errors.New("bad length")
	}

	if _, err := i.reader.Discard(n); err != nil {
		return nil, invalidOffset, invalidOffset, errors.Wrap(err, "discard failed")
	}

	message := make([]byte, messageLength) // TODO: buffer pooling
	if _, err = io.ReadFull(i.reader, message); err != nil {
		return nil, invalidOffset, invalidOffset, errors.Wrap(err, "read failed")
	}

	i.offset += int64(n) + int64(messageLength)

	return message, messageOffset, i.offset, nil
}

func (i *Iterator) nextWait() error {
	if i.endOffset >= i.file.maxSize {
		return io.EOF
	}

	i.file.mutex.Lock()
	defer i.file.mutex.Unlock()

	currentOffset := atomic.LoadInt64(&i.file.offset)
	for currentOffset == i.endOffset && atomic.LoadUint32(&i.file.term) == i.term {
		if atomic.LoadUint32(&i.closed) == 1 {
			return ErrIteratorClosed
		}
		i.file.cond.Wait()
		currentOffset = atomic.LoadInt64(&i.file.offset)
	}

	if atomic.LoadUint32(&i.file.term) != i.term {
		return ErrIteratorInvalid
	}

	i.reader.Reset(io.NewSectionReader(i.file.file, i.endOffset, currentOffset-i.endOffset))
	i.endOffset = currentOffset

	return nil
}

func (i *Iterator) Close() error {
	atomic.StoreUint32(&i.closed, 1)
	i.file.cond.Broadcast()
	return nil
}
