+++
title = "Clustering"
description = "Running fault-tolerant clusters of broker nodes"
date = 2018-12-28T16:16:19+01:00
weight = 40
draft = false
bref = "This guide will tell you how to create local 3-node cluster of EventterMQ brokers in Docker. The same concepts can be applied when creating distributed clusters."
toc = true
+++

EventterMQ is designed from the start to be run as a distributed service. Assumption is that the brokers in the cluster can reach each other over low-latency LAN. More complex cluster topologies (e.g. different regions, different availability zones in a single region) are not implemented yet, so at the moment, it isn't recommended to run cluster over WAN.

### Network topology

Brokers are expected to be run in a trusted private network. Therefore they do not use authentication or encryption for inter-broker communication. For our local cluster we will create such new network in Docker:

```bash
$ docker network create emq
```

> **Note: Docker Swarm**
>
> If you used [Docker Swarm](https://docs.docker.com/engine/swarm/), you could create `overlay` network driver and start containers on separate machines. Then you would have truly distributed cluster of brokers.

### Discovery

Brokers run [SWIM](http://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) gossip protocol for node discovery. This protocol uses **both TCP & UDP** on port 16000 (or different port you choose at the broker startup), therefore configure your firewall rules accordingly.

To create local cluster, first create directory for persistent data:

```bash
$ mkdir -p `pwd`/data
```

Then start the first broker:

```bash
$ docker run -d \
    --network emq \
    -v `pwd`/data:/data \
    --name emq-0 \
    eventter/mq \
    --dir /data/0 \
    --advertise-host emq-0
```

The broker will write its data to `/data/0` directory, bind to all interfaces on default port (i.e. 16000) and tell other nodes that it can be reached on host named `emq-0`. Docker network ensures that the host `emq-0` resolves to the IP address of the container.

It's important that the broker reports to other nodes the right address it can be reached on. Configure this using `--host` & `--advertise-host` arguments:

Arguments | `--host` NOT specified | `--host` specified |
----------|--------------------|------------------------|
<span style="white-space:nowrap;font-weight:bold;">`--advertise-host` NOT specified</span> | The broker listens on all interfaces and advertises to other brokers canonical hostname as reported by operating system. | The broker listens on the IP address or hostname specified by `--host` option and advertises this value to other nodes. |
<span style="white-space:nowrap;font-weight:bold;">`--advertise-host` specified</span> | The broker listens on all interfaces and advertises to other brokers the value specified by `--advertise-host`. **Recommended for most cases.** | The broker listens on the IP address or hostname specified by `--host` option and advertises to other brokers value specified by `--advertise-host`. |

`--dir` and `--advertise-host` is pretty much all you need to specify for broker to start.

Next start other brokers:

```bash
$ docker run -d \
    --network emq \
    -v `pwd`/data:/data \
    --name emq-1 \
    eventter/mq \
    --dir /data/1 \
    --advertise-host emq-1 \
    --join emq-0:16000
$ docker run -d \
    --network emq \
    -v `pwd`/data:/data \
    --name emq-2 \
    eventter/mq \
    --dir /data/2 \
    --advertise-host emq-2 \
    --join emq-0:16000
```

`--join` specifies `host:port` combination of any node in the cluster.

### Cluster state

Nodes share between themselves a structure called **cluster state**. Cluster state contains node addresses, topics, consumer groups etc. This structure is agreed on using consensus algorithm called [Raft](https://raft.github.io/). You can view the current value using `dump` CLI command (it issues RPC request to the node and prints response):

```bash
$ docker run --rm --network emq --name emq-client eventter/mq --host emq-0 dump
=== Cluster state ===
index: 60
namespaces: <
  name: "default"
>
nodes: <
  id: 4788019685657542095
  address: "172.19.0.2:16000"
  state: ALIVE
>
nodes: <
  id: 10681121214306649065
  address: "172.19.0.4:16000"
  state: ALIVE
>
nodes: <
  id: 11191473106259109224
  address: "172.19.0.3:16000"
  state: ALIVE
>


=== Segments ===

```

As you can see, cluster state contains three nodes forming the cluster. (Note: Your node IDs and addresses may differ.) If you use `dump` on any node, you should see the same result.

Now if you, for example, create a topic:

```bash
$ docker run --rm --network emq --name emq-client eventter/mq --host emq-0 create-topic my-topic
```

It will be reflected in cluster state on any node (see that request to create a topic was issued to node `emq-0`, and dump to node `emq-2`):

```bash
$ docker run --rm --network emq --name emq-client eventter/mq --host emq-2 dump
=== Cluster state ===
index: 823
namespaces: <
  name: "default"
  topics: <
    name: "my-topic"
    shards: 1
    replication_factor: 3
    retention: <
      nanos: 1
    >
    default_exchange_type: "fanout"
  >
>
nodes: <
  id: 4788019685657542095
  address: "172.19.0.2:16000"
  state: ALIVE
>
nodes: <
  id: 10681121214306649065
  address: "172.19.0.4:16000"
  state: ALIVE
>
nodes: <
  id: 11191473106259109224
  address: "172.19.0.3:16000"
  state: ALIVE
>


=== Segments ===


```

### What next?

Clients can connect to any node in the cluster and their requests will be forwarded to appropriate node that contains related data (e.g. open topic segments when publishing new message).

Learn more about [protocols]({{< ref "/docs/protocols.md" >}}) you can use. Or read about basic concepts in [Getting Started]({{< ref "/docs/getting-started.md" >}}).