package segments

import (
	"io/ioutil"
	"os"
	"testing"
	"time"
)

func TestDir_Open(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	// open 1
	f1, err := dir.Open(1)
	if err != nil {
		t.Fatal(err)
	}
	if f1.rc != 1 {
		t.Fatalf("expected reference count to be 1, got: %d", f1.rc)
	}

	// open 2
	f2, err := dir.Open(2)
	if err != nil {
		t.Fatal(err)
	}
	if f2 == f1 {
		t.Fatal("different IDs must NOT return same instance")
	}
	if f2.rc != 1 {
		t.Fatalf("expected reference count to be 1, got: %d", f2.rc)
	}

	// open 1 again
	f1_1, err := dir.Open(1)
	if err != nil {
		t.Fatal(err)
	}
	if f1 != f1_1 {
		t.Fatal("same IDs must return same instance")
	}
	if f1.rc != 2 {
		t.Fatalf("expected reference count to be 2, got: %d", f1.rc)
	}
	if f1_1.rc != 2 {
		t.Fatalf("expected reference count to be 2, got: %d", f1_1.rc)
	}

	// release 2
	if err := dir.Release(f2); err != nil {
		t.Fatal(err)
	}
	if f2.rc != 0 {
		t.Fatalf("expected reference count to be 0, got: %d", f1.rc)
	}

	// release 1
	if err := dir.Release(f1_1); err != nil {
		t.Fatal(err)
	}
	if f1_1.rc != 1 {
		t.Fatalf("expected reference count to be 1, got: %d", f1.rc)
	}

	// release 1 again
	if err := dir.Release(f1); err != nil {
		t.Fatal(err)
	}
	if f1.rc != 0 {
		t.Fatalf("expected reference count to be 0, got: %d", f1.rc)
	}
}

func TestDir_Exists(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	if dir.Exists(1) != false {
		t.Fatal("segment should not exist")
	}

	if f, err := dir.Open(1); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	if dir.Exists(1) != true {
		t.Fatal("segment should exist as its in idle set")
	}
}

func TestDir_Exists_NoIdle(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	if dir.Exists(1) != false {
		t.Fatal("segment should not exist")
	}

	if f, err := dir.Open(1); err != nil {
		t.Fatal(err)
	} else {
		f.Close()
	}

	if dir.Exists(1) != true {
		t.Fatal("segment should exist as it was opened before")
	}
}

func TestDir_Exists_NewDir(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	func() {
		dir, err := NewDir(tmpDir, 0755, 0644, 1024, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()

		if dir.Exists(1) != false {
			t.Fatal("segment should not exist")
		}

		if f, err := dir.Open(1); err != nil {
			t.Fatal(err)
		} else {
			f.Close()
		}
	}()

	func() {
		dir, err := NewDir(tmpDir, 0755, 0644, 1024, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer dir.Close()

		if dir.Exists(1) != true {
			t.Fatal("segment should exist now")
		}
	}()
}

func TestDir_List(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	doTestDir_List(t, dir, 0)
}

func doTestDir_List(t *testing.T, dir *Dir, expectedReferenceCount int) {
	m := make(map[uint64]bool)
	for i := 1; i <= 16; i++ {
		f, err := dir.Open(uint64(i))
		if err != nil {
			t.Fatal(err)
		}
		if err := dir.Release(f); err != nil {
			t.Fatal(err)
		}
		m[uint64(i)] = true
	}

	infos, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}

	for _, info := range infos {
		if !m[info.ID] {
			t.Fatalf("segment %d wasn't opened", info.ID)
		}
		delete(m, info.ID)

		if info.Size != 1 {
			t.Fatalf("expected size to be 1, got: %d", info.Size)
		}
		if info.ReferenceCount != expectedReferenceCount {
			t.Fatalf("expected rc to be %d, got: %d", expectedReferenceCount, info.ReferenceCount)
		}
	}

	if len(m) != 0 {
		t.Fatalf("expected to list all opened segments")
	}
}

func TestDir_List_NoIdle(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	doTestDir_List(t, dir, -1)
}

func TestDir_Remove(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dir, err := NewDir(tmpDir, 0755, 0644, 1024, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()

	f, err := dir.Open(1)
	if err != nil {
		t.Fatal(err)
	}
	if err := dir.Release(f); err != nil {
		t.Fatal(err)
	}

	infos, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 {
		t.Fatalf("expected 1 file(s), got: %d", len(infos))
	}

	if err := dir.Remove(1); err != nil {
		t.Fatal(err)
	}

	newInfos, err := dir.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(newInfos) != 0 {
		t.Fatalf("expected 0 file(s), got: %d", len(newInfos))
	}
}
