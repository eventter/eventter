# EventterMQ

[![GitLab Build Status](https://img.shields.io/gitlab/pipeline/eventter/eventter.svg)](https://gitlab.com/eventter/eventter/pipelines)
[![Docker Image on Docker Hub](https://img.shields.io/docker/pulls/eventter/mq.svg)](https://hub.docker.com/r/eventter/mq/)

> Distributed message queue & streaming server

## Overview

- [Website](https://eventter.io/)
- [Documentation](https://eventter.io/docs/)
- [Getting Started](https://eventter.io/docs/getting-started/)
- [Issues](https://github.com/eventter/eventter)

## Contributing

Contributions are welcome and very much appreciated, whether they're new features, bugfixes, or improvements to documentation.

To contribute:

- Start by forking the project on GitHub.
- Push changes to a feature branch.
- Create a [pull request](https://github.com/eventter/eventter/compare).
- For your changes to be accepted, please sign the [CLA](https://cla-assistant.io/eventter/eventter).
- All code should be formatted using `go fmt`.
- All tests must pass.

## Development

The project uses [Go modules](https://github.com/golang/go/wiki/Modules), so you need [Go 1.11](https://golang.org/dl/) or newer.

Otherwise you can do everything the usual way with the Go toolchain.

```sh
# install
go install ./bin/eventtermq
# test
go test ./mq/...
# fix code style
go fmt ./mq/...
```

There is also a `Makefile` that wraps some usual workflows.

```sh
# install
make install
# test
make test
# fix code style
make fmt
# generate sources, code style, run `go vet`, test & install
make all install
```

### Generated code

There are several Go source files that are generated from other sources. Most notably all `*.pb.go` files contain generated [gRPC](https://grpc.io/) (de)serialization & stubs. To generate these files, run `make generate`.

## License

Apache License, Version 2.0 with Commons Claus condition, see [LICENSE file](LICENSE.md).
