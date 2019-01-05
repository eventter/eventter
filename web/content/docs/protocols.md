+++
title = "Protocols"
description = "Which protocols does EventterMQ support?"
date = 2018-12-27T15:34:04+01:00
weight = 30
draft = false
bref = "Which protocols does EventterMQ support?"
toc = true
+++

EventterMQ currently supports three protocols:

- [AMQP 0.9.1]({{< ref "/docs/amqp-0-9-1.md" >}}),
- [AMQP 1.0]({{< ref "/docs/amqp-1-0.md" >}}),
- [gRPC](https://grpc.io/) service described by `emq.proto` ([see it on GitHub](https://github.com/eventter/eventter/tree/master/mq/emq/emq.proto)).

### AMQP 0.9.1

AMQP 0.9.1 is a binary protocol intended for messaging. It's reasonably easy to implement, therefore there are many client libraries. It's most well-know to be the core protocol of [RabbitMQ](http://www.rabbitmq.com/). Probably most up-to-date list of client libraries can be found on RabbitMQ website [here](https://www.rabbitmq.com/devtools.html). Some notable mentions:

- [amqp.node](https://github.com/squaremo/amqp.node) - Node.JS (Javascript) client library,
- [RabbitMQ Java Client](https://github.com/rabbitmq/rabbitmq-java-client/) - Java client library, developed by RabbitMQ,
- [BunnyPHP](https://github.com/jakubkulhan/bunny) - PHP client library,
- [streadway/amqp](https://github.com/streadway/amqp) - Go client library,
- [Bunny](http://rubybunny.info/) - Ruby client library,
- [Lapin](https://github.com/sozu-proxy/lapin) - Rust client library,
- [RabbitMQ C](https://github.com/alanxz/rabbitmq-c) - C client library.

> **Note: Keywords to use**
>
> As you can see most libraries are very "rabbit-themed", therefore if you search for a library in your language, try keywords, apart from **amqp**, such as **rabbitmq**, **rabbit**, **bunny**, **lapin** etc.

---

> **Note 2: Protocol extensions**
>
> RabbitMQ [extends](https://www.rabbitmq.com/extensions.html) AMQP 0.9.1 in several ways. The only RabbitMQ extension supported by EventterMQ is [basic.nack](https://www.rabbitmq.com/nack.html). This extension is reported by EventterMQ when opening new connection, however, some client libraries might not consider server-reported capabilities and take all RabbitMQ extensions for granted.

AMQP uses slightly different model for decoupling producers and consumers than EventterMQ. To see how AMQ entities map to EventterMQ entities read through separate [AMQP 0.9.1 article]({{< ref "/docs/amqp-0-9-1.md" >}}).

### AMQP 1.0

AMQP 1.0 defines a binary protocol for reliable message delivery. It doesn't, however, provide any means to create topics or consumer groups. You might know AMQP 1.0 as one of the protocols supported by [ActiveMQ](https://activemq.apache.org/).

Learn how to connect using AMQP 1.0 in a [separate article]({{< ref "/docs/amqp-1-0.md" >}}).

### gRPC

gRPC API is described by `emq.proto` ([see it on GitHub](https://github.com/eventter/eventter/tree/master/mq/emq/emq.proto)). You can use this API to manage namespaces, topics, consumer groups, and to work with messages (publish & consume). To see how to use this service definition file to generate client library, see [gRPC quick start](https://grpc.io/docs/quickstart/).

There is [Go](https://golang.org/) client (generated from service definition above) available as package `eventter.io/mq/emq`. To get the library, run:

```bash
$ go get -u eventter.io/mq/emq
```

Example usage:

{{< example "examples/grpc-client/client" >}}

### Additional protocols


Support for additional protocols is planned. At the moment planned protocols are:

- HTTP - simple protocol, although not intended for messaging (if you look into `emq.proto`, you'll see that all RPC calls are annotated with HTTP verbs and paths, it should be easy to generate HTTP service using [grpc-gateway](https://github.com/grpc-ecosystem/grpc-gateway)).
- MQTT - protocol mostly used for [IoT](https://en.wikipedia.org/wiki/Internet_of_things) and compute-/memory-constrained devices.

### What next?

Learn about [use cases]({{< ref "/docs/use-cases.md" >}}) of EventterMQ. Or how it achieves fault-tolerance using [clustering]({{< ref "/docs/clustering.md" >}}).
