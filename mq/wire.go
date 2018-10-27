//+build wireinject

package mq

import (
	"github.com/google/go-cloud/wire"
	"github.com/hashicorp/memberlist"
	"github.com/dgraph-io/badger"
)

func createMemberlistConfig(config *Config) *memberlist.Config {
	cfg := memberlist.DefaultLANConfig()
	cfg.BindAddr = config.BindHost
	cfg.BindPort = config.Port
	return cfg
}

func createMemberlist(config *memberlist.Config) (*memberlist.Memberlist, func(), error) {
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, nil, err
	}
	return list, func() { list.Shutdown() }, err
}

func Open(config *Config) (*Server, func(), error) {
	panic(
		wire.Build(
			wire.NewSet(
				createMemberlist,
				createMemberlistConfig,
				NewServer,
			),
		),
	)
}

func OpenDatabase(config *Config) (*badger.DB, func(), error) {
	opts := badger.DefaultOptions
	db, err := badger.Open(opts)
	if err != nil {
		return err
	}

	return db, func() { db.Close() }, err
}
