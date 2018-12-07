package segments

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestOpen(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, t.Name())

	f, err := Open(path, 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	reopenedF, err := Open(path, 0644, 1024)
	if err != nil {
		t.Fatal(err)
	}

	if err := reopenedF.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestFile_Write(t *testing.T) {
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

	if err := f.Write([]byte("bar")); err != nil {
		t.Fatal(err)
	}

	iterator, err := f.Read(false)
	if err != nil {
		t.Fatal(err)
	}
	messagesRead := 0
	for {
		message, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		messagesRead++

		if string(message) != "bar" {
			t.Fatalf("wrong message read")
		}
	}

	if messagesRead != 1 {
		t.Fatalf("wrong number of messages read")
	}
}

func TestFile_Sum(t *testing.T) {
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

	if err := f.Write([]byte("hello, world")); err != nil {
		t.Fatal(err)
	}

	sum, size, err := f.Sum(sha1.New(), SumAll)
	if err != nil {
		t.Fatal(err)
	}
	expectedSum := "841cd25736208650f307bb48d90f9b9a493fe842"
	if gotSum := hex.EncodeToString(sum); gotSum != expectedSum {
		t.Fatalf("sha1 sum expected: %s, got: %s", expectedSum, gotSum)
	}
	expectedSize := int64(14)
	if size != expectedSize {
		t.Fatalf("size expected: %d, got: %d", expectedSize, size)
	}

	if err := f.Write([]byte("foo bar")); err != nil {
		t.Fatal(err)
	}

	newSum, newSize, err := f.Sum(sha1.New(), SumAll)
	if err != nil {
		t.Fatal(err)
	}
	expectedNewSum := "71041e8c2c79f53170f9c756790d0bd4062e6e4c"
	if gotSum := hex.EncodeToString(newSum); gotSum != expectedNewSum {
		t.Fatalf("sha1 sum expected: %s, got: %s", expectedNewSum, gotSum)
	}
	expectedNewSize := int64(22)
	if newSize != expectedNewSize {
		t.Fatalf("size expected: %d, got: %d", expectedNewSize, newSize)
	}

	originalSum, _, err := f.Sum(sha1.New(), size)
	if gotSum := hex.EncodeToString(originalSum); gotSum != expectedSum {
		t.Fatalf("sha1 sum expected: %s, got: %s", expectedSum, gotSum)
	}
}

func TestFile_Truncate(t *testing.T) {
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

	for i := 1; i <= 10; i++ {
		if err := f.Write([]byte(strconv.Itoa(i))); err != nil {
			t.Fatal(err)
		}
	}

	iterator, err := f.Read(false)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for {
		_, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		n++
	}
	if n != 10 {
		t.Fatalf("expected 10 messages, got: %d", n)
	}

	if err := f.Truncate(TruncateAll); err != nil {
		t.Fatal(err)
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
	n = 0
	for {
		_, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}

		n++
	}
	if n != 3 {
		t.Fatalf("expected 10 messages, got: %d", n)
	}
}
