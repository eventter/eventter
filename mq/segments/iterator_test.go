package segments

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
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// write 10 messages
	offsets := make([]int64, 11)
	for i := 1; i <= 10; i++ {
		offsets[i] = f.offset
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	// read 10 messages back
	iterator, err := f.Read(false)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for i := 1; ; i++ {
		message, offset, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		n++

		if got := string(message); strconv.Itoa(i) != got {
			t.Fatalf("expected: %d, got: %s", i, got)
		}

		if offset != offsets[i] {
			t.Fatalf("offset - expected: %d, got: %d", offsets[i], offset)
		}
	}

	if n != 10 {
		t.Fatalf("expected 10 messages, got %d", n)
	}

	// write another 10 messages
	for i := 11; i <= 20; i++ {
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	// read 20 messages back
	iterator, err = f.Read(false)
	if err != nil {
		t.Fatal(err)
	}
	n = 0
	for i := 1; ; i++ {
		message, _, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		n++

		if got := string(message); strconv.Itoa(i) != got {
			t.Fatalf("expected: %d, got: %s", i, got)
		}
	}

	if n != 20 {
		t.Fatalf("expected 20 messages, got %d", n)
	}
}

func TestIterator_Next_Wait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Fatal(err)
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
				t.Fatalf("could not write message %d: %v", i, err)
			}
		}
	}()

	iterator, err := f.Read(true)
	if err != nil {
		t.Fatal(err)
	}
	n := 1
	for {
		message, offset, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		if got := string(message); strconv.Itoa(n) != got {
			t.Fatalf("expected: %d, got: %s", n, got)
		}

		if offset != offsets[n] {
			t.Fatalf("expected offset: %d, got: %d", offsets[n], offset)
		}

		n++
	}

	if n != expectedN {
		t.Fatalf("expected to read %d messages, got %d", expectedN, n)
	}
}

func TestIterator_Next_TruncateNoWait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	iterator, err := f.Read(false)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.Truncate(TruncateAll); err != nil {
		t.Fatal(err)
	}
	_, _, _, err = iterator.Next()
	if err != ErrIteratorInvalid {
		t.Fatalf("expected error %v, got %v", ErrIteratorInvalid, err)
	}

	for i := 1; i <= 3; i++ {
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	iterator, err = f.Read(false)
	if err != nil {
		t.Fatal(err)
	}

	data, _, _, err := iterator.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "1" {
		t.Fatalf("expected %s, got %s", "1", string(data))
	}

	if err := f.Truncate(iterator.offset); err != nil {
		t.Fatal(err)
	}

	_, _, _, err = iterator.Next()
	if err != ErrIteratorInvalid {
		t.Fatalf("expected error %v, got %v", ErrIteratorInvalid, err)
	}
}

func TestIterator_Next_TruncateWait(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	iterator, err := f.Read(true)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(1 * time.Millisecond)
		if err := f.Truncate(TruncateAll); err != nil {
			t.Fatal(err)
		}
	}()

	_, _, _, err = iterator.Next()
	if err != ErrIteratorInvalid {
		t.Fatalf("expected error %v, got %v", ErrIteratorInvalid, err)
	}
}

func TestIterator_Close(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	iterator, err := f.Read(true)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(1 * time.Millisecond)
		iterator.Close()
	}()

	_, _, _, err = iterator.Next()
	if err != ErrIteratorClosed {
		t.Fatalf("expected error %v, got %v", ErrIteratorClosed, err)
	}
}
