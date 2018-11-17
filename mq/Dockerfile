FROM golang:1.11

COPY . /src

RUN set -ex \
    && cd /src \
    && CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"' -v -o /bin/eventtermq ./cmd/eventtermq

FROM scratch
COPY --from=0 /bin/eventtermq /eventtermq
ENTRYPOINT ["/eventtermq"]
