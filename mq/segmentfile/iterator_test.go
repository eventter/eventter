package segmentfile

import (
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestIterator_Next_NoWait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	// write 10 messages
	offsets := make([]int64, 11)
	for i := 1; i <= 10; i++ {
		offsets[i] = f.offset
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	// read 10 messages back
	iterator, err := f.Read(false)
	if err != nil {
		t.Error(err)
	}
	n := 0
	for i := 1; ; i++ {
		message, offset, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		n++

		if got := string(message); strconv.Itoa(i) != got {
			t.Errorf("expected: %d, got: %s", i, got)
		}

		if offset != offsets[i] {
			t.Errorf("offset - expected: %d, got: %d", offsets[i], offset)
		}
	}

	if n != 10 {
		t.Errorf("expected 10 messages, got %d", n)
	}

	// write another 10 messages
	for i := 11; i <= 20; i++ {
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Error(err)
		}
	}

	// read 20 messages back
	iterator, err = f.Read(false)
	if err != nil {
		t.Error(err)
	}
	n = 0
	for i := 1; ; i++ {
		message, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		n++

		if got := string(message); strconv.Itoa(i) != got {
			t.Errorf("expected: %d, got: %s", i, got)
		}
	}

	if n != 20 {
		t.Errorf("expected 20 messages, got %d", n)
	}
}

func TestIterator_Next_Wait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	expectedN := 284
	offsets := make([]int64, expectedN+1)

	go func() {
		time.Sleep(1 * time.Millisecond)

		// write until full
		for i := 1; ; i++ {
			offsets[i] = f.offset
			err := f.Write([]byte(strconv.Itoa(i)))
			if err == ErrFull {
				break
			} else if err != nil {
				t.Errorf("could not write message %d: %v", i, err)
			}
		}
	}()

	iterator, err := f.Read(true)
	if err != nil {
		t.Error(err)
	}
	n := 1
	for {
		message, offset, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		if got := string(message); strconv.Itoa(n) != got {
			t.Errorf("expected: %d, got: %s", n, got)
		}

		if offset != offsets[n] {
			t.Errorf("expected offset: %d, got: %d", offsets[n], offset)
		}

		n++
	}

	if n != expectedN {
		t.Errorf("expected to read %d messages, got %d", expectedN, n)
	}
}

func TestIterator_Close(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	iterator, err := f.Read(true)
	if err != nil {
		t.Error(err)
	}

	go func() {
		time.Sleep(1 * time.Millisecond)
		iterator.Close()
	}()

	n := 0
	for {
		_, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		n++
	}

	expectedN := 0
	if n != expectedN {
		t.Errorf("expected to read %d messages, got %d", expectedN, n)
	}
}
