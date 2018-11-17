package segmentfile

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestOpen(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	path := filepath.Join(tmpDir, t.Name())

	f, err := Open(path, 0644, 1024)
	if err != nil {
		t.Error(err)
	}

	if err := f.Close(); err != nil {
		t.Error(err)
	}

	reopenedF, err := Open(path, 0644, 1024)
	if err != nil {
		t.Error(err)
	}

	if err := reopenedF.Close(); err != nil {
		t.Error(err)
	}
}

func TestFile_Write(t *testing.T) {
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

	if err := f.Write([]byte("bar")); err != nil {
		t.Error(err)
	}

	iterator := f.Read(false)
	messagesRead := 0
	for {
		message, _, err := iterator.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		messagesRead++

		if string(message) != "bar" {
			t.Errorf("wrong message read")
		}
	}

	if messagesRead != 1 {
		t.Errorf("wrong number of messages read")
	}
}

func TestFile_Sum(t *testing.T) {
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

	if err := f.Write([]byte("hello, world")); err != nil {
		t.Error(err)
	}

	sum, size, err := f.Sum(sha1.New())
	if err != nil {
		t.Error(err)
	}

	expectedSum := "841cd25736208650f307bb48d90f9b9a493fe842"
	if gotSum := hex.EncodeToString(sum); gotSum != expectedSum {
		t.Errorf("sha1 sum expected: %s, got: %s", expectedSum, gotSum)
	}

	expectedSize := int64(14)
	if size != expectedSize {
		t.Errorf("size expected: %d, got: %d", expectedSize, size)
	}
}
