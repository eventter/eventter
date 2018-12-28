+++
title = "Getting Started"
description = "How to start the broker, send, and receive messages"
date = 2018-12-27T14:30:01+01:00
weight = 10
draft = false
bref = "In this tutorial you will learn how to install the broker, how it approaches messaging, and how to send and receive messages"
toc = true
+++

### Installation

EventterMQ doesn't have a stable version yet. You can use latest Docker image that is automatically built from the latest source code, or you can build the broker binary yourself.

#### Docker

Docker image can be [pulled from Docker Hub](https://hub.docker.com/r/eventter/mq/).

```sh
$ docker pull eventter/mq
```

To start the broker, run:

```sh
$ docker run --name emq-broker -p 16000:16000 -d eventter/mq
```

#### Build from source

You will need:

- [Git](https://git-scm.com/downloads)
- [Go](https://golang.org/dl/) (1.11 or newer)

First download source code from the project public repository:

```sh
$ git clone https://github.com/eventter/eventter.git
```

Then go to the root of the downloaded repository and build `eventtermq` binary:

```sh
$ go build -o eventtermq ./bin/eventtermq
```

To start the broker, run:

```sh
$ eventermq --dir ./data
```

### Messaging overview

The broker supports multiple protocols for sending and receiving messages, see [Protocols](/docs/protocols/) to learn about which protocols are supported. In this article we will use its native [gRPC](https://grpc.io/) API through CLI interface.

The CLI uses the same binary as the broker. If you started the broker in Docker using the command above, please add following alias to your shell:

```sh
$ alias eventtermq="docker run --name emq-client --link emq-broker --rm eventter/mq --host emq-broker"
```

If you used other messaging products, you might be used to different models how messages are sent:

1. First, simplest, approach is that messages are sent to **queues** (linear ordered data structure, messages are **pushed to the front**) as well as received from these queues (messages are **popped from the back**, i.e. from first to last). There is 1-to-1 relationship between producers and consumers.
2. If you want to **decouple** producers and consumers, you need to add a **level of indirection**. For example [AMQP 0.9.1](/docs/amqp-0-9-1/) (which is also supported by EventterMQ, see linked article to learn how you can communicate with the broker using AMQP) does this by adding so-called **exchanges**. Producers send messages to exchanges. Exchanges, through defined rules, route messages to zero or more queues. Each queue receives a **copy** of the messages.
3. Or you can do it the other way around, messages are sent to **topics** (similar to queues in that they're linear ordered data structure, messages are **pushed to the front**, however, unlike queues messages can be **read from any point**, not only popped from the back). Then client(s) that want to receive messages form **consumer groups**. Each consumer group reads each message from the topic once.

EventterMQ chose third approach. Its benefits are:

- each message is stored once (not copied to multiple queues),
- topics retain messages and may be read multiple times,
- consumer groups may start reading past messages, not only new ones.

### Create topic

So first you need to create a topic:

```sh
$ eventtermq create-topic my-topic --shards 1 --replication-factor 3
{
  "ok": true,
  "index": 15
}
```

> **Note: Namespaces**
>
> EventterMQ is designed to be multi-tenant. Topics (and consumer groups as well) are scoped by namespaces. By default on a new cluster an empty namespace called `default` is created.
>
> CLI commands use this namespace by default. If you want to create different namespace, run:
>
> ```sh
$ eventtermq create-namespace my-namespace
```
>
> Then add option `--namespace` to CLI invocations, e.g.:
>
> ```sh
$ eventtermq create-topic my-topic --namespace my-namespace --shards 1 --replication-factor 3 --retention 24h
```

#### Shards

Topics are comprised of segments. Segments are where messages are physically stored. Topic has zero or more segments that are open. Open segments are the ones that new messages are appended to. `--shards` option configures how many open segments topic can have. The more open segments topic has, greater the throughput of messages. However, if there are more than one open segment, ordering of the messages is not guaranteed.

#### Replication factor

Segments are limited in size (64 MiB). If open segment reaches size limit, no new messages can be written to it and it gets **rotated** (i.e. it gets closed and a new segment is opened instead). Segments act as a unit of replication. `--replication-factor` specifies how many copies of data are there to be in the cluster. If you use replication factor of 1, there is only one copy of the data, so if the broker node fails, you can loose messages.

Replication factor 3 means that there are 3 copies. For an open segment it means that one node acts as a primary - it receives new messages and appends them to the segment as well as responds to the read requests -, and 2 more nodes (if there are) replicate data written to the primary. If the primary fails, one of the replicas (one that is most up-to-date) is chosen to act as a new primary.

When segments is closed, any node that with fully replicated data can act as primary - read requests are routed randomly to such nodes.

#### Retention period

Hard drives and SSDs are cheap. However, storage is not infinite. Therefore if topics could only grow in size by rotating segments, it would be impractical. Retention period specifies for how long closed segments will be kept around before deleting their data files. In the example above, retention period is set to 24 hours. Therefore, if segment was closed longer than 24 hours ago and no consumer group reads from it anymore (either it wasn't read by any consumer, or it was entirely consumed by all consumer groups), it will be deleted.

If you set retention to zero, it means that data will be retained forever. If you want to delete segments as soon as possible, set retention to lowest possible positive value - 1 nanosecond.

Open segments are never deleted, even if they exceed specified retention period. Therefore, if you don't use topic for storage, only for messaging, you can estimate its total disk usage as `shards * 64 MiB`.

### Send message

After you've created a topic, you can send messages to it (in messaging parlance, **publish** messages to it):

```sh
$ eventtermq publish my-topic "hello, world"
{
  "ok": true
}
```

### Create consumer group

As written earlier, messages are received by **consumer groups**. To create consumer group, run:

```sh
$ eventtermq create-consumer-group my-cg --bind my-topic --since -1h --size 1024
{
  "ok": true,
  "index": 34
}
```

Consumer group is a task that runs on one of the nodes in the cluster and manages reading messages from bound topics. Consumer group may be bound to no topic - if so, consumer won't every receive any message.

#### Bindings

Binding connects consumer group to a topic and specifies what messages read from the topic's segments will be sent to consumers. Binding contains name of the topic (topic must exist at the time the consumer group is being created), **exchange type** (and possibly for certain exchange types additional data).

Exchange types are those as defined in [AMQP 0.9.1](/docs/amqp-0-9-1/):

- **fanout** exchange type matches all messages,
- **direct** compares binding's **routing key** with message's routing key - if routing keys are equal, the message matches,
- **topic** is similar to _direct_, it compares _routing keys_, however, topic routing keys are separated by periods, and instead of specific words, they may contain wildcards (`*` matches any single word, `#` matches zero or more words),
- **headers** compares message's headers whether they match.

If any binding matches the message, it's sent to consumers. If multiple bindings would match, message is sent only once.

When creating consumer group from CLI, `--bind` creates _fanout_ binding, `--bind-direct` _direct_ binding, and `--bind-topic` _topic_ binding. _Headers_ exchange type is not used very often, and so cannot be created from CLI. If you want to create consumer group with _headers_ exchange type, use directly one of [supported protocols](/docs/protocols/).

#### Since

Topics retain messages for the specified retention period. Consumer group, when created, may start to read messages only new messages, or start from past point in time (or event future point in time).

If you set CLI option `--since -1h`, messages will be sent to consumers as long as they're newer than 1 hour ago.

#### Size

When consumer group runs, before it sends messages to consumers, it reads messages from all relevant segments into memory. `--size` specified how many messages will be kept in memory at the time.

Once a message is sent to the consumer, it's marked to be in processing. But it's still kept in consumer group's message buffer until consumer:

- **cancels** its subscription (also happens when connection fails / process dies) - message is then re-sent to other consumers,
- sends **acknowledgement** (_ack_) - message is free'd from consumer group buffer and marked never to be sent other consumer,
- sends **rejection** (_nack_) - message is re-sent to other consumers.

Therefore there cannot be more messages _in-flight_ than the size of the consumer group. Default consumer group size is `1024`.

### Receive message

Finally when you want to receive messages from consumer group, you create subscription:

```sh
$ eventtermq subscribe my-cg
{
  "topic": {
    "namespace": "default",
    "name": "my-topic"
  },
  "message": {
    "data": "aGVsbG8sIHdvcmxk"
  }
}
^C
```

> **Note: CLI**
>
> Messages are treated by the broker as opaque binary data. CLI outputs data in JSON as Base64-serialized string. When you decode `aGVsbG8sIHdvcmxk`, you will get `hello, world` sent earlier.
>
> CLI `subscribe` command creates subscription in auto-acknowledgement mode, so every message sent to the client will be automatically marked as acknowledged and won't be ever sent to any other consumer again.

### What next?

In your application code you probably don't want to communicate with the broker by executing CLI commands. Learn about [protocols](/docs/protocols/) the broker supports and libraries to use.

Do you still hesitate whether EventterMQ is right tool for the job? Read about its [use cases](/docs/use-cases/).
