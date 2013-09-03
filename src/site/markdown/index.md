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

Navigating the Documentation
----------------------------

### Conceptual Understanding

[Concepts / Terminology](./Concepts.html)

[Architecture](./Architecture.html)

### Hands-on Helix

[Quickstart](./Quickstart.html)

[Tutorial](./Tutorial.html)

[Javadocs](http://helix.incubator.apache.org/apidocs/index.html)

### Recipes

[Distributed lock manager](./recipes/lock_manager.html)

[Rabbit MQ consumer group](./recipes/rabbitmq_consumer_group.html)

[Rsync replicated file store](./recipes/rsync_replicated_file_store.html)

[Service discovery](./recipes/service_discovery.html)

[Distributed Task DAG Execution](./recipes/task_dag_execution.html)

[User-Defined Rebalancer Example](./recipes/user_def_rebalancer.html)


What Is Helix
--------------
Helix is a generic _cluster management_ framework used for the automatic management of partitioned, replicated and distributed resources hosted on a cluster of nodes. 


What Is Cluster Management
--------------------------
To understand Helix, first you need to understand _cluster management_.  A distributed system typically runs on multiple nodes for the following reasons:

* scalability
* fault tolerance
* load balancing

Each node performs one or more of the primary function of the cluster, such as storing and serving data, producing and consuming data streams, and so on.  Once configured for your system, Helix acts as the global brain for the system.  It is designed to make decisions that cannot be made in isolation.  Examples of such decisions that require global knowledge and coordination:

* scheduling of maintainence tasks, such as backups, garbage collection, file consolidation, index rebuilds
* repartitioning of data or resources across the cluster
* informing dependent systems of changes so they can react appropriately to cluster changes
* throttling system tasks and changes

While it is possible to integrate these functions into the distributed system, it complicates the code.  Helix has abstracted common cluster management tasks, enabling the system builder to model the desired behavior with a declarative state model, and let Helix manage the coordination.  The result is less new code to write, and a robust, highly operable system.


Key Features of Helix
---------------------
1. Automatic assignment of resources and partitions to nodes
2. Node failure detection and recovery
3. Dynamic addition of resources 
4. Dynamic addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic load balancing and throttling of transitions
7. Optional pluggable rebalancing for user-defined assignment of resources and partitions


Why Helix
---------
Modeling a distributed system as a state machine with constraints on states and transitions has the following benefits:

* Separates cluster management from the core functionality of the system.
* Allows a quick transformation from a single node system to an operable, distributed system.
* Increases simplicity: system components do not have to manage a global cluster.  This division of labor makes it easier to build, debug, and maintain your system.


Build Instructions
------------------

Requirements: JDK 1.6+, Maven 2.0.8+

```
    git clone https://git-wip-us.apache.org/repos/asf/incubator-helix.git
    cd incubator-helix
    mvn install package -DskipTests 
```

Maven dependency

```
    <dependency>
      <groupId>org.apache.helix</groupId>
      <artifactId>helix-core</artifactId>
      <version>0.6.1-incubating</version>
    </dependency>
```

[Download](./download.html) Helix artifacts from here.
   
Publications
-------------

* Untangling cluster management using Helix at [SOCC Oct 2012](http://www.socc2012.org/home/program)  
    - [paper](https://915bbc94-a-62cb3a1a-s-sites.googlegroups.com/site/acm2012socc/helix_onecol.pdf)
    - [presentation](http://www.slideshare.net/KishoreGopalakrishna/helix-socc-v10final)
* Building distributed systems using Helix Apache Con Feb 2013
    - [presentation at ApacheCon](http://www.slideshare.net/KishoreGopalakrishna/apache-con-buildingddsusinghelix)
    - [presentation at VMWare](http://www.slideshare.net/KishoreGopalakrishna/apache-helix-presentation-at-vmware)
* Data driven testing:
    - [short talk at LSPE meetup](http://www.slideshare.net/KishoreGopalakrishna/data-driven-testing)
    - [paper DBTest 2013 acm SIGMOD:will be published on Jun 24, 2013](http://dbtest2013.soe.ucsc.edu/Program.htm)

