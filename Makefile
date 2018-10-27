all:
	# nothing

proto:
	cd mq; protoc --gogofaster_out=plugins=grpc:. discovery.proto
