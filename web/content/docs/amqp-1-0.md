+++
title = "AMQP 1.0"
description = "How to connect to the broker using AMQP 1.0"
date = 2019-01-05T21:45:21+01:00
weight = 60
draft = false
bref = "Learn how to use AMQP 1.0 to work with the broker."
toc = true
+++

EventterMQ offers support for all [AMQP versions]({{< ref "/docs/protocols.md" >}}) on the non-standard port that defaults to port of the gRPC service + 1. Default gRPC port is 16000, so default AMQP port is 16001.

{{< example "examples/amqp-1-0/connect" >}}

At the moment, the broker doesn't require any client authentication. Although if you try to connect using `PLAIN` (username & password) or `ANONYMOUS` SASL mechanisms, the connection gets through and authentication always succeeds.

{{< example "examples/amqp-1-0/connect_sasl" >}}

### Sessions

AMQP 1.0 is a multiplexed protocol. On a single TCP connection you can create multiple _sessions_. Each session defines incoming / outgoing windows for flow control.

{{< example "examples/amqp-1-0/session" >}}

### Send messages

To publish messages to a topic you create sender _link_ on the session. The link needs target address. The address has the form `/{namespace}/{topic}`. So if you want to publish message to the topic named `my-topic` in the namespace `my-ns`, use address `/my-ns/my-topic`.

{{< example "examples/amqp-1-0/sender" >}}

### Receive messages

To subscribe to messages from a consumer group you create receiver link on the session. The link uses source address of the consumer group that has form `/{namespace}/{consumer-group}`. E.g. to subscribe to consumer group named `my-cg` in the namespace `my-ns`, use address `/my-ns/my-cg`. When the client acts as the receiving end of the link, it can specify _link credit_, which is max number of in-flight messages.

The default sender mode of the link is _unsettled_. This setting offers **at-least-once** delivery guarantee - after you process the message you need to accept (the message will be acknowledged and won't be delivered to another client), or release the message (it will be delivered to another client). You can choose **at-most-once** delivery using _settled sender mode_.

{{< example "examples/amqp-1-0/receiver" >}}

> **Note: Exactly-once**
>
> AMQP 1.0 also offers exactly-once delivery guarantee, however, this delivery guarantee is not implemented by the broker.

### What next?

Learn how the broker achieves fault-tolerance using [clustering]({{< ref "/docs/clustering.md" >}}). Or about [other protocols]({{< ref "/docs/protocols.md" >}}) the broker supports.
