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
  <title>Tutorial - YAML Cluster Setup</title>
</head>

## [Helix Tutorial](./Tutorial.html): YAML Cluster Setup

As an alternative to using Helix Admin to set up the cluster, its resources, constraints, and the state model, Helix supports bootstrapping a cluster configuration based on a YAML file. Below is an annotated example of such a file for a simple distributed lock manager where a lock can only be LOCKED or RELEASED, and each lock only allows a single participant to hold it in the LOCKED state.

```
clusterName: lock-manager-custom-rebalancer # unique name for the cluster (required)
resources:
  - name: lock-group # unique resource name (required)
    rebalancer: # required
      mode: USER_DEFINED # required - USER_DEFINED means we will provide our own rebalancer
      class: org.apache.helix.userdefinedrebalancer.LockManagerRebalancer # required for USER_DEFINED
    partitions:
      count: 12 # number of partitions for the resource (default is 1)
      replicas: 1 # number of replicas per partition (default is 1)
    stateModel:
      name: lock-unlock # model name (required)
      states: [LOCKED, RELEASED, DROPPED] # the list of possible states (required if model not built-in)
      transitions: # the list of possible transitions (required if model not built-in)
        - name: Unlock
          from: LOCKED
          to: RELEASED
        - name: Lock
          from: RELEASED
          to: LOCKED
        - name: DropLock
          from: LOCKED
          to: DROPPED
        - name: DropUnlock
          from: RELEASED
          to: DROPPED
        - name: Undrop
          from: DROPPED
          to: RELEASED
      initialState: RELEASED # (required if model not built-in)
    constraints:
      state:
        counts: # maximum number of replicas of a partition that can be in each state (required if model not built-in)
          - name: LOCKED
            count: "1"
          - name: RELEASED
            count: "-1"
          - name: DROPPED
            count: "-1"
        priorityList: [LOCKED, RELEASED, DROPPED] # states in order of priority (all priorities equal if not specified)
      transition: # transitions priority to enforce order that transitions occur
        priorityList: [Unlock, Lock, Undrop, DropUnlock, DropLock] # all priorities equal if not specified
participants: # list of nodes that can serve replicas (optional if dynamic joining is active, required otherwise)
  - name: localhost_12001
    host: localhost
    port: 12001
  - name: localhost_12002
    host: localhost
    port: 12002
  - name: localhost_12003
    host: localhost
    port: 12003
```

Using a file like the one above, the cluster can be set up either with the command line:

```
helix/helix-core/target/helix-core/pkg/bin/YAMLClusterSetup.sh localhost:2199 lock-manager-config.yaml
```

or with code:

```
YAMLClusterSetup setup = new YAMLClusterSetup(zkAddress);
InputStream input =
    Thread.currentThread().getContextClassLoader()
        .getResourceAsStream("lock-manager-config.yaml");
YAMLClusterSetup.YAMLClusterConfig config = setup.setupCluster(input);
```

Some notes:

- A rebalancer class is only required for the USER_DEFINED mode. It is ignored otherwise.

- Built-in state models, like OnlineOffline, LeaderStandby, and MasterSlave, or state models that have already been added only require a name for stateModel. If partition and/or replica counts are not provided, a value of 1 is assumed.
