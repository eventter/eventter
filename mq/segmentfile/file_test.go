package segmentfile

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"eventter.io/mq/client"
)

func TestOpen(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segmentfile")
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
	tmpDir, err := ioutil.TempDir("", "segmentfile")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	writeMessage := &client.Message{
		RoutingKey: "foo",
		Data:       []byte("bar"),
	}

	if err := f.Write(writeMessage); err != nil {
		t.Error(err)
	}

	iterator := f.Read(false)
	readMessage := &client.Message{}
	messagesRead := 0
	for {
		err := iterator.Next(readMessage)
		if err == io.EOF {
			break
		} else if err != nil {
			t.Error(err)
		}

		messagesRead++

		if readMessage.RoutingKey != writeMessage.RoutingKey {
			t.Errorf("wrong routing key read")
		}

		if string(readMessage.Data) != string(writeMessage.Data) {
			t.Errorf("wrong data read")
		}
	}

	if messagesRead != 1 {
		t.Errorf("wrong number of messages read")
	}
}

func TestFile_Sum(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "segmentfile")
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(tmpDir)

	f, err := Open(filepath.Join(tmpDir, t.Name()), 0644, 1024)
	if err != nil {
		t.Error(err)
	}
	defer f.Close()

	msg := &client.Message{
		Data: []byte("hello, world"),
	}

	if err := f.Write(msg); err != nil {
		t.Error(err)
	}

	sum, size, err := f.Sum(sha1.New())
	if err != nil {
		t.Error(err)
	}

	expectedSum := "a11f718b31266ab02933b324ee457713b29545b8"
	if gotSum := hex.EncodeToString(sum); gotSum != expectedSum {
		t.Errorf("sha1 sum expected: %s, got: %s", expectedSum, gotSum)
	}

	expectedSize := int64(16)
	if size != expectedSize {
		t.Errorf("size expected: %d, got: %d", expectedSize, size)
	}
}
