//+build !production

package livereload

type master struct {
	config *Config
}

func newMaster(config *Config) (Master, error) {
	return &master{config}, nil
}

func (*master) Run() error {
	return nil
}
