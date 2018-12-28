+++
title = "Architecture"
description = "Design doc"
date = 2018-12-28T17:46:02+01:00
weight = 80
draft = false
bref = "Original design doc for the broker with technical requirements"
toc = true
+++

EventterMQ is a cloud-native message queue / distributed log broker.

**Why cloud-native?**

*   Distributed by default - running a single node is just a cluster with 1 member.
*   Minimal configuration, 12-factor app.
*   Nodes can go away, be dynamically removed, added etc. with cluster being still available.

**Why message queue / distributed log?**

*   Support for multiple protocols for publishing and consuming messages
    *   gRPC - main protocol, also used for inter-node communication
    *   AMQP 0.9.1 (initial release)
    *   AMQP 1.0 (later)
    *   HTTP (later)
    *   MQTT (later)
*   Messages are published to topics that act as a distributed log with configurable retention.
*   Consuming from topics and even distribution of messages is handled by the broker.

### Discovery, gossip

*   SWIM implemented by [https://github.com/hashicorp/memberlist](https://github.com/hashicorp/memberlist).

### Ingestion

*   All nodes can receive new messages on topics.
*   Messages in a topic are saved in segment files. Every segment file is identified by its ID.
*   Segments can be open, or closed.
*   Node tries to find open segment of the topic where the data should go:
    *   First it tries local open segment.
        *   If the segment is not full, it appends the message to the segment. Message is then copied by segment replicas.
        *   If the segment is full, RPC is sent to leader to rotate segments.
            *   Rotation can be completely successful - old segment is closed, new segment is created, and the message gets written to a new segment.
            *   Partially successful - old segment is closed, new segment is not created. The node then continues as there was no opened segment.
    *   Then it queries cluster state for open segments for given topic.
        *   If there are less open segments than number of shards configured for topic, the node opens new segment with leader.
            *   This can fail, as another node might've opened new segment in between, the leader reports error with open segments assignments. The current node selects one of the nodes with open segments and forwards the message to it.
            *   If the operation succeeds, the node writes the message to newly opened segment.
*   Persistence modes are configurable per publish - either success is returned right away, or after the message is written to the segment and it was fsynced, or after # of segment replicas responded that the replica was synced.
*   Segment is rotated after it reaches size of 64 MiB.
*   Segments, whose time when the segment was closed, falls below topic's retention period are periodically removed.
*   Open segments have single primary node and zero or more replicating nodes.
*   When segment gets closed, primary node is placed in done node set. After replicas catch up with done replica (previously primary), they report to leader that they've completed. Leader moves replicas from replicating set to done set.
*   When replication factor for the topic changes, leader adds nodes to replicating set, or removes from done set. After concerned node detects the change in cluster state, it starts replication, or discards segment data.


### Consumer groups



*   Consumer groups deliver messages to the consumers in round-robin fashion.
*   One group can be bound to zero or more topics. Bindings can have additional properties that filter messages from given topic.
*   Consumer group is assigned topic segments according to bindings.
*   Each consumer group reads messages from assigned segments (either locally, or remotely) - pull method.
*   Consumer group sends window updates to receive additional messages. When no consumers are connected to the shard, the window is zero, so ingest nodes don't send any messages.
*   Window is first updated for nodes with older messages.
*   Consumer group maintains transient buffer of messages.
*   Consumer maintains persistent logs of committed segment offsets - all messages in given segment with offset lower to committed offset were ack'ed by consumers.
*   If message M is sent to consumer, M is removed from buffer and added to in-flight set (IFS). When ACK is received for message N, N is removed from IFS. If all offsets in IFS and head of buffer are greater than offset of N, offset gets committed.
*   When consumer disconnects, all its messages are removed from IFS and added back to buffer.
*   Offset commits are first written to a consumer group offset commits segment. When segment needs to be rotated, because it exceeded segment size limit, consumer group first updates cluster state with latest committed offsets and then rotates the segment. Closed segment is then automatically deleted as offset commits were already persisted in cluster state.

### Consumers

*   Clients can connect to any node in the cluster to start consuming messages.
*   Subscription is created in consumer group - again, pull method.
*   Subscription can have limited size (to allow even distribution between consumers).
*   Client send (N)ACKs for received messages. These messages can arrive to any node - they are forwarded to appropriate node running the consumer group.
*   When client disconnects, subscription is closed and all messages are returned to the consumer group to be consumed by different clients.

### Cluster state

*   Nodes use algorithm for distributed consensus with elected leader - [Raft](https://raft.github.io/), implemented by https://github.com/hashicorp/raft.
*   Commands to alter cluster state (e.g. creating new topic, removing consumer group, updating offset commits) are always issued to leader. It returns success only after majority of the nodes has accepted the change.
*   Master assigns segment replicas & consumer groups to live nodes in the cluster. If a node goes down for more than X seconds, master re-assigns the resources to different nodes in the cluster.
