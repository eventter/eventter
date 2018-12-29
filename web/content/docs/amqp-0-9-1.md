+++
title = "AMQP 0.9.1"
description = "How to connect to the broker using AMQP 0.9.1"
date = 2018-12-28T13:00:38+01:00
weight = 50
draft = false
bref = "AMQP 0.9.1 chooses a slightly different messaging model than EventterMQ. Learn about AMQ model and how it maps to EvennterMQ's model."
toc = true
+++

In [messaging overview]({{< ref "/docs/getting-started.md#overview" >}}) we've learnt about two distinct models that allow decoupling of message producers and consumers. As said in the overview, AMQP chose **exchange-queue** model, but EventterMQ uses **topic-consumer group** model. In this article we'll describe AMQ model in depth, explain how it maps to topics & consumer groups and show examples how to connect to the broker using AMQP.

> **Note: Advanced Message Queueing Protocol**
>
> When this article refers to AMQP, it refers to [AMQP 0.9.1](http://www.amqp.org/specification/0-9-1/amqp-org-download). There's also [AMQP 1.0](http://www.amqp.org/specification/1.0/amqp-org-download). Although sharing the same name, these are, in essence, completely different protocols. Both define binary protocols for messaging use cases, however, AMQP 0.9.1 defines shared semantics for AMQ entities and how client apps and brokers interact to send and receive messages, AMQP 1.0 centers around connection establishment, multiplexing and flow control, and leaves semantics of entities up to the concrete implementations.

### AMQ model

AMQP starts with defining certain entities brokers support and how client apps may interact with these. AMQP brokers are meant to be multi-tenant, therefore all entities of AMQ model are scoped by **virtual hosts** (or _vhosts_ for short). Virtual hosts naturally map to EventterMQ's namespaces. Every client connection works with exactly one virtual host. Most client libraries default to virtual host names `/` (just slash). Because `/` isn't a valid namespace name in EventterMQ, it gets translated to namespace named `default`. AMQP doesn't allow client apps to create new virtual hosts or delete existing virtual hosts. To create namespaces, ergo AMQP virtual hosts, use [`create-namespace` CLI command]({{< ref "/docs/getting-started.md#create-topic" >}}).

Messages in AMQ world are published to **exchanges**. Exchanges route messages to **queues**. Consuming apps then read messages from queues. Exchange do not store messages, they only act as a layer that looks at message _envelope_, which consists of **message properties** (statically defined by the protocol) and **message headers** (dynamic key-value pairs attached to the message by the sending app), compares the information using its **exchange type** with **queue bindings**, and finally **pushes** message to be **stored by matching queues**.

> **Note: RabbitMQ's `x-delayed` exchange**
>
> In RabbitMQ certain exchange types can in fact store messages. For example, [delayed exchange plugin](https://github.com/rabbitmq/rabbitmq-delayed-message-exchange) adds exchange type `x-delayed` that routes messages to appropriate queues after the time specified by `x-delay` message header has passed. In the meantime, the message is stored by the exchange, not any queue.

There are different exchange types defined by the specification:

- **fanout** - simplest exchange type - routes all messages to all bound queues,
- **direct** - looks at **routing key** message property and if it exactly matches routing key specified by the binding, routes message to the bound queue,
- **topic** - also works with routing key, however, uses wildcard matching with `*` and `#` as _word_ placeholders (words are separated by dots, `*` means single word, `#` means zero or more words; e.g. `foo.*` matches `foo.bar`, `foo.baz`, but doesn't match `foo.qux.xyz`; `foo.#` matches `foo`, `foo.bar`, `foo.baz` as well as `foo.qux.xyz`, but doesn't match `bar.xyz`),
- **headers** - matches _message headers_ with **binding arguments** (dynamic key-value pairs as well).

EventterMQ chose exactly opposite model. Messages are stored by **topics** and then **pulled** by **consumer groups** using bindings. Albeit the models differ, AMQ model can be represented by EventterMQ's. Exchanges are represented as topics and queues as consumer groups.

One slight difference is that in AMQ, bindings act according to type of the exchange and _routing key_, or _arguments_ specified in binding. Consumer group bindings specify all of _exchange type_, _routing key_, and _arguments_. For interoperability reasons, topics define **default exchange type** that is set to consumer group bindings if not specified.

### Security

At the moment, EventterMQ doesn't require client authentication to manage broker entities or publish messages. AMQP connections, on the other hand, require client authentication. The broker does SASL authentication handshake using `PLAIN` and `AMQPLAIN` mechanisms, both require client to specify username and password, that are used by client libraries, however, it accepts any combination of username and password.

### Connection

IANA-assigned port for AMQP is 5672. EventterMQ by default doesn't use this port. Instead it chooses port for its [gRPC]({{< ref "/docs/protocols.md#grpc" >}}) service + 1. Default port for gRPC is 16000, so **default port for AMQP is 16001**.

See [list of AMQP 0.9.1 client libraries]({{< ref "/docs/protocols.md#amqp-0-9-1" >}}).

{{< example "examples/amqp-0-9-1/connect" >}}

Creating new TCP connections is costly, therefore AMQP is a multiplexed protocol - it allows multiple **channels** of communication to be present on the same underlying connection, think of channels as lightweight connections.

To do actual work, you must open a new channel on the connection.

{{< example "examples/amqp-0-9-1/channel" >}}

### Exchanges (topics)

Once you've opened a channel, you can create exchanges (i.e. topics).

{{< example "examples/amqp-0-9-1/exchange" >}}

AMQP defines several knobs that can be used when creating exchanges:

- **durable** - If set, it means that the exchange should survive server restart, transient exchanges should be purged upon server restart. Since EventterMQ is designed to be highly available, there is no such thing "server restart", that could wipe all memory data, e.g. transient exchanges. Therefore you have to set this parameter to true, otherwise the broker returns not-implemented error.
- **auto-delete** - If it, it means that the exchange should be deleted when there are queues consuming from it. This argument was introduced in earlier versions of the spec and deprecated in AMQP 0.9.1. If set to true, the broker returns not-implemented error.
- **internal** - Internal exchanges could be used to more intricate routing topologies, they make sense for [dead-lettering](https://www.rabbitmq.com/dlx.html) and extensions such as [exchange to exchange bindings](https://www.rabbitmq.com/e2e.html). As the broker supports neither of those, you cannot declare internal exchanges, it returns not-implemented error if you try.

Additional topic settings can by configured by **arguments** map.

AMQP spec requires that the broker declares special exchanges like `amq.direct`, `amq.topic` etc. EventterMQ doesn't declare these exchanges (topics).

### Queues (consumer groups)

{{< example "examples/amqp-0-9-1/queue" >}}

Queues have several knobs as well. Non-**durable** and **auto-delete** queues are not implemented for the same reasons as mentioned in previous section. **exclusive** queues are also not implemented as it would require brokers to share list of all connected clients and whether they subscribe to consumer groups. If you need exclusive access to a queue, I suggest you use external locking service.

If you leave queue name empty, one will be generated and returned in response.

#### Bindings

EventterMQ's [`CreateConsumerGroup` RPC call]({{< ref "/docs/protocols.md#grpc" >}}) both creates consumer group and its bindings. AMQP separates these operations - after you've created a consumer group, you have to call `queue.bind`:

{{< example "examples/amqp-0-9-1/bind" >}}

Additionally, AMQP server always ensures that there is an **default (nameless) exchange** of type _direct_ and every queue is bound to this exchange using its names as a routing key. This exchanges is backed by a topic with single shard, replication factor of 3 and lowest possible retention period (1 nanosecond, so essentially messages will be immediately forgotten once consumed).

### Producers

To send message directly to the queue, use empty string as exchange name and queue name as _routing key_:

{{< example "examples/amqp-0-9-1/publish_queue" >}}

> Note: **mandatory** and **immediate** knobs must always be false, otherwise not-implemented error will be returned.

Publishing to named exchange can be done as follows:

{{< example "examples/amqp-0-9-1/publish_exchange" >}}

### Consumers

To start receiving messages, you call consume method on a channel:

{{< example "examples/amqp-0-9-1/consume" >}}

Again consume have various knobs. **exclusive** (no other consumer can bound to the queue) and **no-local** (messages sent by this connection cannot be received) are not implemented. **no-ack** means that you do not have to send acknowledgements for processed messages (you get _at-most-once_ delivery guarantee) and is implemented.

### What next?

Learn how the broker achieves fault-tolerance using [clustering]({{< ref "/docs/clustering.md" >}}). Or about [other protocols]({{< ref "/docs/protocols.md" >}}) the broker supports.
