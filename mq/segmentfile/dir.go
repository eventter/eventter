package segmentfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Dir struct {
	dirName         string
	dirPerm         os.FileMode
	filePerm        os.FileMode
	maxSize         int64
	idleTimeout     time.Duration
	mutex           sync.Mutex
	fileMap         map[uint64]*File
	referenceCounts map[*File]int
	idlingSince     map[*File]time.Time
	closeC          chan struct{}
}

func NewDir(dirName string, dirPerm os.FileMode, filePerm os.FileMode, maxSize int64, idleTimeout time.Duration) (*Dir, error) {
	if idleTimeout < 0 {
		return nil, errors.New("idle timeout must not be negative")
	}

	if err := os.MkdirAll(dirName, dirPerm); err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}

	d := &Dir{
		dirName:         dirName,
		dirPerm:         dirPerm,
		filePerm:        filePerm,
		maxSize:         maxSize,
		idleTimeout:     idleTimeout,
		fileMap:         make(map[uint64]*File),
		referenceCounts: make(map[*File]int),
		idlingSince:     make(map[*File]time.Time),
		closeC:          make(chan struct{}),
	}

	if idleTimeout > 0 {
		go d.closeIdle()
	}

	return d, nil
}

func (d *Dir) closeIdle() {
	ticker := time.NewTicker(d.idleTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.mutex.Lock()
			t := time.Now().Add(-d.idleTimeout)
			var files []*File
			for file, since := range d.idlingSince {
				if since.Before(t) {
					files = append(files, file)
				}
			}
			for _, file := range files {
				d.closeFile(file)
			}
			d.mutex.Unlock()
		case <-d.closeC:
			return
		}
	}
}

func (d *Dir) Open(id uint64) (*File, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if file, ok := d.fileMap[id]; ok {
		d.referenceCounts[file] += 1
		delete(d.idlingSince, file)
		return file, nil
	}

	name := fmt.Sprintf("%016x", id)
	path := filepath.Join(d.dirName, name[14:16], name)
	pathDir := filepath.Dir(path)
	if err := os.MkdirAll(pathDir, d.dirPerm); err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}
	file, err := NewFile(id, path, d.filePerm, d.maxSize)
	if err != nil {
		return nil, errors.Wrap(err, "creating segment file failed")
	}

	d.fileMap[id] = file
	d.referenceCounts[file] = 1
	delete(d.idlingSince, file)

	return file, nil
}

func (d *Dir) Release(file *File) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	rc, ok := d.referenceCounts[file]
	if !ok {
		return errors.New("segment file does not belong to this pool")
	}

	rc -= 1

	if rc > 0 {
		d.referenceCounts[file] = rc
		return nil

	} else if d.idleTimeout > 0 {
		d.referenceCounts[file] = rc
		d.idlingSince[file] = time.Now()
		return nil

	} else {
		return d.closeFile(file)
	}
}

func (d *Dir) closeFile(file *File) error {
	delete(d.fileMap, file.id)
	delete(d.referenceCounts, file)
	delete(d.idlingSince, file)
	return file.Close()
}

func (d *Dir) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var files []*File
	for _, file := range d.fileMap {
		files = append(files, file)
	}
	for _, file := range files {
		d.closeFile(file)
	}

	close(d.closeC)

	return nil
}
