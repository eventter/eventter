+++
title = "Use Cases"
description = "Overview of popular use cases"
date = 2018-12-27T16:11:09+01:00
weight = 20
draft = false
bref = "Learn what EventterMQ features make it good for particular use cases"
toc = true
+++

Here goes list of use cases EventterMQ is good for.

### Job queues

When you need to process reliably jobs that can fail, are resource-intensive, need to have limited concurrency, or whose producers are written in different programming language (platform) than consumers, you probably will use some sort of messaging broker. Popular products for this are [RabbitMQ](http://www.rabbitmq.com/) and [ActiveMQ](http://activemq.apache.org/).

EventterMQ can be used for this as well. It supports standardized messaging protocols like [AMQP 0.9.1]({{< ref "/docs/amqp-0-9-1.md" >}}) (like RabbitMQ) and (plans to support) AMQP 1.0 (like ActiveMQ), so it can be used in-place replacement of these messaging brokers.

Unlike many single-purpose job queues and general messaging brokers, EventterMQ is designed from the start to be run in distributed fashion to be highly available even when servers fail.

### Logging (planned)

Today's applications generate a lot of operational messages that need to be stored for certain period of time, regularly processed by other applications and infrequently searched in by people. EventterMQ's topics with configurable retention period are great for that. You can create different topics for different types of log messages, different applications etc., with several retention periods - e.g. retain error logs forever, but access logs only for couple of days. Namespaces can be used for multi-tenancy.

What you usually need to do with logs is from time to time search in them, e.g. for errors, then list nearby messages, aggregate counts of matching messages etc. EventterMQ plans to add support for indexing on topics of JSON-encoded values.

### Event tracking (planned)

Website and mobile-app user activity tracking (event tracking) generates heaps of data. Storing this type of data reliably, densely and on the budget often requires a lot of infrastructure to be set up. EventterMQ's storage encoding adds negligible overhead to raw stored data. Replication ensures that data are safely saved even in case of server/disk failures. EventterMQ plans to add support for another type of retention period - archival retention period. Archived segments will be compressed and moved to, mostly cheaper, object storage solutions like AWS S3, Google Cloud Storage etc. There they can reside until they need to be accessed again.

### What next?

Learn about [protocols]({{< ref "/docs/protocols.md" >}}) the broker supports. Or how it achieves fault-tolerance using [clustering]({{< ref "/docs/clustering.md" >}}).
