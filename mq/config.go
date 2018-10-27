package mq

import (
	"math/rand"
	"os"
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

	if c.ID == 0 {
		// TODO: parse from dir first
		c.ID = rand.New(rand.NewSource(time.Now().UnixNano())).Uint64()
	} else {
		// FIXME: check
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

	if err := os.MkdirAll(c.Dir, c.DirPerm); err != nil {
		return err
	}

	return nil
}
