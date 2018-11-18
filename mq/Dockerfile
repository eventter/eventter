FROM golang:1.11

COPY . /src

RUN set -ex \
    && cd /src \
    && CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"' -v -o /bin/eventtermq ./bin/eventtermq

FROM scratch
COPY --from=0 /bin/eventtermq /eventtermq
VOLUME ["/data"]
EXPOSE 16000/tcp 16000/udp
ENTRYPOINT ["/eventtermq"]
CMD ["--dir", "/data"]
