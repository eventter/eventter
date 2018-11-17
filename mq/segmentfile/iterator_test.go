package segmentfile

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"eventter.io/mq/client"
)

func TestIterator_Next_NoWait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segmentfile")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(1, filepath.Join(tmpDir, "1"), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	msg := &client.Message{}

	// write 10 messages
	for i := 1; i <= 10; i++ {
		msg.Reset()
		msg.Data = []byte(strconv.Itoa(i))
		if err := f.Write(msg); err != nil {
			t.Error(err)
		}
	}

	// read 10 messages back
	iterator := f.Read(false)
	n := 0
	for i := 1; ; i++ {
		err := iterator.Next(msg)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		n++

		if got := string(msg.Data); strconv.Itoa(i) != got {
			t.Errorf("expected: %d, got: %s", i, got)
		}
	}

	if n != 10 {
		t.Errorf("expected 10 messages, got %d", n)
	}

	// write another 10 messages
	for i := 11; i <= 20; i++ {
		msg.Reset()
		msg.Data = []byte(strconv.Itoa(i))
		if err := f.Write(msg); err != nil {
			t.Error(err)
		}
	}

	// read 20 messages back
	iterator = f.Read(false)
	n = 0
	for i := 1; ; i++ {
		err := iterator.Next(msg)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		n++

		if got := string(msg.Data); strconv.Itoa(i) != got {
			t.Errorf("expected: %d, got: %s", i, got)
		}
	}

	if n != 20 {
		t.Errorf("expected 20 messages, got %d", n)
	}
}

func TestIterator_Next_Wait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segmentfile")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(1, filepath.Join(tmpDir, "1"), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	go func() {
		time.Sleep(1 * time.Millisecond)

		// write until full
		for i := 1; ; i++ {
			msg := &client.Message{Data: []byte(strconv.Itoa(i))}
			err := f.Write(msg)
			if err == ErrFull {
				break
			} else if err != nil {
				t.Errorf("could not write message %d: %v", i, err)
			}
		}
	}()

	msg := &client.Message{}

	iterator := f.Read(true)
	n := 1
	for {
		err := iterator.Next(msg)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		if got := string(msg.Data); strconv.Itoa(n) != got {
			t.Errorf("expected: %d, got: %s", n, got)
		}

		n++
	}

	expectedN := 190
	if n != expectedN {
		t.Errorf("expected to read %d messages, got %d", expectedN, n)
	}
}
