<!---
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<head>
  <title>Home</title>
</head>


## Apache Helix

Apache Helix is a generic _cluster management_ framework used for the automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. __Helix automates reassignment of resources in the face of node failure and recovery, cluster expansion, and reconfiguration.__


## What Is Cluster Management?

To understand Helix, you first need to understand __cluster management__. A distributed system typically runs on multiple nodes for the following reasons:

* scalability
* fault tolerance
* load balancing

Each node performs one or more of the primary functions of the cluster, such as storing and serving data, producing and consuming data streams, and so on. __Once configured for your system, Helix acts as the global brain for the system__. It is designed to make decisions that cannot be made in isolation.  Examples of such decisions that require global knowledge and coordination:

* scheduling of maintainence tasks, such as backups, garbage collection, file consolidation, index rebuilds
* repartitioning of data or resources across the cluster
* informing dependent systems of changes so they can react appropriately to cluster changes
* throttling system tasks and changes

While it is possible to integrate these functions into the distributed system, it complicates the code. Helix has abstracted common cluster management tasks, enabling the system builder to model the desired behavior with a declarative state model, and let Helix manage the coordination. __The result is less new code to write, and a robust, highly operable system__.

## What does Helix provide?

* Automatic assignment of resources and partitions to nodes
* Node failure detection and recovery
* Dynamic addition of resources
* Dynamic addition of nodes to the cluster
* Pluggable distributed state machine to manage the state of a resource via state transitions
* Automatic load balancing and throttling of transitions
* Optional pluggable rebalancing for user-defined assignment of resources and partitions


## Why Helix?

Modeling a distributed system as a state machine with constraints on states and transitions has the following benefits:

* Separates cluster management from the core functionality of the system.
* Allows a quick transformation from a single node system to an operable, distributed system.
* Increases simplicity: system components do not have to manage a global cluster.  This division of labor makes it easier to build, debug, and maintain your system.

---

### Join the Conversation

[Bay Area Meetup Group](http://www.meetup.com/Building-distributed-systems-using-Apache-Helix-Meetup-group/)

[`#apachehelix`](./IRC.html)

[`user@helix.apache.org`](mailto:user@helix.apache.org)

### News

Apache Helix has two new releases:

* [0.6.5](./0.6.5-docs/index.html) - A release that includes stability improvements.

    [\[Quick Start\]](./0.6.5-docs/Quickstart.html) [\[Release Notes\]](./releasenotes/release-0.6.5.html)

* [0.7.1 (beta)](./0.7.1-docs/index.html) - A release that includes YARN integration, ad-hoc task management, and performant IPC.

    [\[Quick Start\]](./0.7.1-docs/Quickstart.html) [\[Release Notes\]](./releasenotes/release-0.7.1.html)

### Download

<a href="./0.6.5-docs/download.html" class="btn btn-primary btn-small">0.6.5</a>

<a href="./0.7.1-docs/download.html" class="btn btn-primary btn-small">0.7.1 (beta)</a>

### Maven Dependency

```
<dependency>
  <groupId>org.apache.helix</groupId>
  <artifactId>helix-core</artifactId>
  <version>0.6.5</version>
</dependency>
```

### Building

Requirements: JDK 1.6+, Maven 2.0.8+

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout helix-0.6.5
mvn install package -DskipTests
```
