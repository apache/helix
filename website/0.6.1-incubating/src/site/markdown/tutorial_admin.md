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

Helix Tutorial: Admin Operations
--------------------------------

Helix provides interfaces for the operator to administer the cluster.  For convenience, there is a command line interface as well as a REST interface.

### Helix Admin Operations

First, make sure you get to the command-line tool, or include it in your shell PATH.

```
cd helix/helix-core/target/helix-core-pkg/bin
```

Get help

```
./helix-admin.sh --help
```

All other commands have this form:

```
./helix-admin.sh --zkSvr <ZookeeperServerAddress (Required)> <command> <parameters>
```

### Commands

Add a new cluster

```
--addCluster <clusterName>
```

Add a new Instance to a cluster

```
--addNode <clusterName> <InstanceAddress (host:port)>
```

Add a State model to a cluster

```
--addStateModelDef <clusterName> <filename>
```

Add a resource to a cluster

```
--addResource <clusterName> <resourceName> <partitionNum> <stateModelRef> <--mode (AUTO_REBALANCE|AUTO|CUSTOMIZED)>
```

Upload an IdealState (Partition to Node Mapping)

```
--addIdealState <clusterName> <resourceName> <filename>
```

Delete a cluster

```
--dropCluster <clusterName>
```

Delete a resource (drop an existing resource from a cluster)

```
--dropResource <clusterName> <resourceName>
```

Drop an existing instance from a cluster

```
--dropNode <clusterName> <InstanceAddress (host:port)>
```

Enable/disable the entire cluster. This will pause the controller, which means no transitions will be triggered, but the existing nodes in the cluster continue to function without any management by the controller.

```
--enableCluster <clusterName> <true/false>
```

Enable/disable an instance. This is useful to take a node out of the cluster for maintenance/upgrade.

```
--enableInstance <clusterName> <InstanceName> <true/false>
```

Enable/disable a partition

```
--enablePartition <clusterName> <instanceName> <resourceName> <partitionName> <true/false>
```

Query information about a cluster

```
--listClusterInfo <clusterName>
```

List existing clusters (remember, Helix can manage multiple clusters)

```
--listClusters
```

Query info of a single instance in a cluster

```
--listInstanceInfo <clusterName> <InstanceName>
```

List instances in a cluster

```
--listInstances <clusterName>
```

Query information about a partition

```
--listPartitionInfo <clusterName> <resourceName> <partitionName>
```

Query information about a resource

```
--listResourceInfo <clusterName> <resourceName>
```

List resources hosted in a cluster

```
--listResources <clusterName>
```

Query information about a state model in a cluster

```
--listStateModel <clusterName> <stateModelName>
```

Query information about state models in a cluster

```
--listStateModels <clusterName>
```

