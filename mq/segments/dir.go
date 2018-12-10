package segments

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
)

const (
	dirBuckets = 64
	fileExt    = ".seg"
)

var (
	ErrInvalidID  = errors.New("segment ID must not be zero")
	ErrInUse      = errors.New("segment still in use")
	ErrNotBelongs = errors.New("segment file does not belong to this pool")
)

type Dir struct {
	dirName     string
	dirPerm     os.FileMode
	filePerm    os.FileMode
	maxSize     int64
	idleTimeout time.Duration
	locks       [dirBuckets]sync.Mutex
	fileMaps    [dirBuckets]map[uint64]*File
	closeC      chan struct{}
}

type FileInfo struct {
	ID             uint64
	Path           string
	Size           int64
	ReferenceCount int
}

func NewDir(dirName string, dirPerm os.FileMode, filePerm os.FileMode, maxSize int64, idleTimeout time.Duration) (*Dir, error) {
	if idleTimeout < 0 {
		return nil, errors.New("idle timeout must not be negative")
	}

	if err := os.MkdirAll(dirName, dirPerm); err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}

	d := &Dir{
		dirName:     dirName,
		dirPerm:     dirPerm,
		filePerm:    filePerm,
		maxSize:     maxSize,
		idleTimeout: idleTimeout,
		closeC:      make(chan struct{}),
	}

	for i := 0; i < dirBuckets; i++ {
		d.fileMaps[i] = make(map[uint64]*File)
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
			for bucket := uint64(0); bucket < dirBuckets; bucket++ {
				d.locks[bucket].Lock()
				t := time.Now().Add(-d.idleTimeout)
				var filesToClose []*File
				for _, file := range d.fileMaps[bucket] {
					if !file.idle.IsZero() && file.idle.Before(t) {
						filesToClose = append(filesToClose, file)
					}
				}
				for _, file := range filesToClose {
					d.closeFile(bucket, file)
				}
				d.locks[bucket].Unlock()
			}
		case <-d.closeC:
			return
		}
	}
}

func (d *Dir) Open(id uint64) (*File, error) {
	if id == 0 {
		return nil, ErrInvalidID
	}

	bucket := id % dirBuckets

	d.locks[bucket].Lock()
	defer d.locks[bucket].Unlock()

	if file, ok := d.fileMaps[bucket][id]; ok {
		file.rc += 1
		file.idle = time.Time{}
		return file, nil
	}

	path := d.getPath(id)
	if err := os.MkdirAll(filepath.Dir(path), d.dirPerm); err != nil {
		return nil, errors.Wrap(err, "mkdir failed")
	}
	file, err := Open(path, d.filePerm, d.maxSize)
	if err != nil {
		return nil, errors.Wrap(err, "creating segment file failed")
	}

	file.id = id
	file.rc = 1
	file.idle = time.Time{}

	d.fileMaps[bucket][id] = file

	return file, nil
}

func (d *Dir) getPath(id uint64) string {
	name := strconv.FormatUint(id, 16)
	if l := len(name); l < 16 {
		name = strings.Repeat("0", 16-l) + name
	}
	return filepath.Join(d.dirName, name[14:16], name+fileExt)
}

func (d *Dir) Exists(id uint64) bool {
	if id == 0 {
		return false
	}

	bucket := id % dirBuckets

	d.locks[bucket].Lock()
	defer d.locks[bucket].Unlock()

	if _, ok := d.fileMaps[bucket][id]; ok {
		return true
	}

	path := d.getPath(id)
	_, err := os.Stat(path)
	return err == nil
}

func (d *Dir) Release(file *File) error {
	if file.id == 0 {
		return ErrInvalidID
	}

	bucket := file.id % dirBuckets

	d.locks[bucket].Lock()
	defer d.locks[bucket].Unlock()

	if _, ok := d.fileMaps[bucket][file.id]; !ok {
		return ErrNotBelongs
	}

	file.rc -= 1

	if file.rc > 0 {
		return nil

	} else if d.idleTimeout > 0 {
		file.idle = time.Now()
		return nil

	} else {
		return d.closeFile(bucket, file)
	}
}

func (d *Dir) List() ([]*FileInfo, error) {
	var infos []*FileInfo

	err := filepath.Walk(d.dirName, func(path string, fileInfo os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if filepath.Ext(path) != fileExt {
			return nil
		}

		baseName := filepath.Base(path)
		id, err := strconv.ParseUint(baseName[:len(baseName)-len(fileExt)], 16, 64)
		if err != nil {
			return err
		}

		bucket := id % dirBuckets
		d.locks[bucket].Lock()
		rc := -1
		if file, ok := d.fileMaps[bucket][id]; ok {
			rc = file.rc
		}
		d.locks[bucket].Unlock()

		infos = append(infos, &FileInfo{
			ID:             id,
			Path:           path,
			Size:           fileInfo.Size(),
			ReferenceCount: rc,
		})

		return nil
	})

	return infos, err
}

func (d *Dir) Remove(id uint64) error {
	if id == 0 {
		return ErrInvalidID
	}

	bucket := id % dirBuckets
	d.locks[bucket].Lock()
	defer d.locks[bucket].Unlock()

	if file, ok := d.fileMaps[bucket][id]; ok {
		if file.rc > 0 {
			return ErrInUse
		}

		if err := d.closeFile(bucket, file); err != nil {
			return errors.Wrap(err, "file close failed")
		}
	}

	if err := os.Remove(d.getPath(id)); err != nil {
		return errors.Wrap(err, "remove failed")
	}

	return nil
}

func (d *Dir) closeFile(bucket uint64, file *File) error {
	delete(d.fileMaps[bucket], file.id)
	return file.Close()
}

func (d *Dir) Close() error {
	close(d.closeC)

	for bucket := uint64(0); bucket < dirBuckets; bucket++ {
		d.locks[bucket].Lock()

		var files []*File
		for _, file := range d.fileMaps[bucket] {
			files = append(files, file)
		}
		for _, file := range files {
			d.closeFile(bucket, file)
		}

		d.locks[bucket].Unlock()
	}

	return nil
}
