DOCKER_TAG=latest

all: generate fmt vet test

dependencies:
	go install -v github.com/gogo/protobuf/protoc-gen-gogofaster

generate: proto

proto:
	cd mq/client; protoc -I. -I$$(go list -m -f '{{ .Dir }}' github.com/gogo/protobuf) --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types:. eventtermq.proto
	cd mq; protoc -I. -I./client -I$$(go list -m -f '{{ .Dir }}' github.com/gogo/protobuf) --gogofaster_out=plugins=grpc,Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,Mclient/eventtermq.proto=eventter.io/mq/client:. cluster_state.proto discovery_rpc.proto node_rpc.proto raft_rpc.proto segments.proto

fmt:
	go fmt $$(go list ./... | grep -v vendor)

download-proto:
	cd mq/client ; \
		rm -rf google googleapis-master ; \
		curl -sSL https://github.com/googleapis/googleapis/archive/master.tar.gz | tar xzf - ; \
		mkdir -p google/api ; \
		cp googleapis-master/google/api/{annotations,http}.proto google/api/ ; \
		rm -rf googleapis-master

vet:
	go vet $$(go list ./... | grep -v vendor)

test:
	go test -v -race $$(go list ./... | grep -v vendor)

install:
	go install -v ./bin/livereload-example
	go install -v ./bin/eventtermq
