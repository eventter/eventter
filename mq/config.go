package mq

import (
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
)

type Config struct {
	ID            uint64
	BindHost      string
	AdvertiseHost string
	Port          int
	Dir           string
	DirPerm       os.FileMode
}

func (c *Config) Init() error {
	if c.Dir == "" {
		return errors.New("dir not set")
	}

	if err := os.MkdirAll(c.Dir, c.DirPerm); err != nil {
		return errors.Wrap(err, "could not create dir")
	}

	idFile := filepath.Join(c.Dir, "id")
	_, err := os.Stat(idFile)
	idFileExists := !os.IsNotExist(err)
	if c.ID == 0 {
		if idFileExists {
			buf, err := ioutil.ReadFile(idFile)
			if err != nil {
				return errors.Wrap(err, "could not read ID from file")
			}
			id, err := NodeIDFromString(strings.Trim(string(buf), " \r\n"))
			if err != nil {
				return errors.Wrap(err, "could not parse ID from file")
			}
			c.ID = id
		} else {
			c.ID = rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()
			err := ioutil.WriteFile(idFile, []byte(NodeIDToString(c.ID)+"\n"), 0644)
			if err != nil {
				return errors.Wrap(err, "could not write ID to file")
			}
		}
	} else if idFileExists {
		buf, err := ioutil.ReadFile(idFile)
		if err != nil {
			return errors.Wrap(err, "could not read ID for check")
		}
		id, err := NodeIDFromString(strings.Trim(string(buf), " \r\n"))
		if err != nil {
			return errors.Wrap(err, "could not parse ID for check")
		}

		if id != c.ID {
			return errors.Errorf("configured with ID [%016x], however, persisted ID is [%016x]", c.ID, id)
		}
	} else {
		err := ioutil.WriteFile(idFile, []byte(NodeIDToString(c.ID)+"\n"), 0644)
		if err != nil {
			return errors.Wrap(err, "could not write ID to file")
		}
	}

	if c.AdvertiseHost == "" {
		if c.BindHost == "" || c.BindHost == "0.0.0.0" || c.BindHost == "[::]" {
			hostname, err := os.Hostname()
			if err != nil {
				return errors.Wrap(err, "could not get hostname")
			}
			c.AdvertiseHost = hostname
		} else {
			c.AdvertiseHost = c.BindHost
		}
	}

	return nil
}
