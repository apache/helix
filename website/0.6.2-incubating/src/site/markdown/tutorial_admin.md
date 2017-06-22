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
  <title>Tutorial - Admin Operations</title>
</head>

## [Helix Tutorial](./Tutorial.html): Admin Operations

Helix provides a set of admin APIs for cluster management operations. They are supported via:

* Java API
* Command Line Interface
* REST Interface via helix-admin-webapp

### Java API
See interface [_org.apache.helix.HelixAdmin_](http://helix.apache.org/javadocs/0.6.2-incubating/reference/org/apache/helix/HelixAdmin.html)

### Command Line Interface
The command line tool comes with helix-core package:

Get the command line tool:

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.6.2-incubating
./build
cd helix-core/target/helix-core-pkg/bin
chmod +x *.sh
```

Get help:

```
./helix-admin.sh --help
```

All other commands have this form:

```
./helix-admin.sh --zkSvr <ZookeeperServerAddress> <command> <parameters>
```

#### Supported Commands

| Command Syntax | Description |
| -------------- | ----------- |
| _\-\-activateCluster \<clusterName controllerCluster true/false\>_ | Enable/disable a cluster in distributed controller mode |
| _\-\-addCluster \<clusterName\>_ | Add a new cluster |
| _\-\-addIdealState \<clusterName resourceName fileName.json\>_ | Add an ideal state to a cluster |
| _\-\-addInstanceTag \<clusterName instanceName tag\>_ | Add a tag to an instance |
| _\-\-addNode \<clusterName instanceId\>_ | Add an instance to a cluster |
| _\-\-addResource \<clusterName resourceName partitionNumber stateModelName\>_ | Add a new resource to a cluster |
| _\-\-addResourceProperty \<clusterName resourceName propertyName propertyValue\>_ | Add a resource property |
| _\-\-addStateModelDef \<clusterName fileName.json\>_ | Add a State model definition to a cluster |
| _\-\-dropCluster \<clusterName\>_ | Delete a cluster |
| _\-\-dropNode \<clusterName instanceId\>_ | Remove a node from a cluster |
| _\-\-dropResource \<clusterName resourceName\>_ | Remove an existing resource from a cluster |
| _\-\-enableCluster \<clusterName true/false\>_ | Enable/disable a cluster |
| _\-\-enableInstance \<clusterName instanceId true/false\>_ | Enable/disable an instance |
| _\-\-enablePartition \<true/false clusterName nodeId resourceName partitionName\>_ | Enable/disable a partition |
| _\-\-getConfig \<configScope configScopeArgs configKeys\>_ | Get user configs |
| _\-\-getConstraints \<clusterName constraintType\>_ | Get constraints |
| _\-\-help_ | print help information |
| _\-\-instanceGroupTag \<instanceTag\>_ | Specify instance group tag, used with rebalance command |
| _\-\-listClusterInfo \<clusterName\>_ | Show information of a cluster |
| _\-\-listClusters_ | List all clusters |
| _\-\-listInstanceInfo \<clusterName instanceId\>_ | Show information of an instance |
| _\-\-listInstances \<clusterName\>_ | List all instances in a cluster |
| _\-\-listPartitionInfo \<clusterName resourceName partitionName\>_ | Show information of a partition |
| _\-\-listResourceInfo \<clusterName resourceName\>_ | Show information of a resource |
| _\-\-listResources \<clusterName\>_ | List all resources in a cluster |
| _\-\-listStateModel \<clusterName stateModelName\>_ | Show information of a state model |
| _\-\-listStateModels \<clusterName\>_ | List all state models in a cluster |
| _\-\-maxPartitionsPerNode \<maxPartitionsPerNode\>_ | Specify the max partitions per instance, used with addResourceGroup command |
| _\-\-rebalance \<clusterName resourceName replicas\>_ | Rebalance a resource |
| _\-\-removeConfig \<configScope configScopeArgs configKeys\>_ | Remove user configs |
| _\-\-removeConstraint \<clusterName constraintType constraintId\>_ | Remove a constraint |
| _\-\-removeInstanceTag \<clusterName instanceId tag\>_ | Remove a tag from an instance |
| _\-\-removeResourceProperty \<clusterName resourceName propertyName\>_ | Remove a resource property |
| _\-\-resetInstance \<clusterName instanceId\>_ | Reset all erroneous partitions on an instance |
| _\-\-resetPartition \<clusterName instanceId resourceName partitionName\>_ | Reset an erroneous partition |
| _\-\-resetResource \<clusterName resourceName\>_ | Reset all erroneous partitions of a resource |
| _\-\-setConfig \<configScope configScopeArgs configKeyValueMap\>_ | Set user configs |
| _\-\-setConstraint \<clusterName constraintType constraintId constraintKeyValueMap\>_ | Set a constraint |
| _\-\-swapInstance \<clusterName oldInstance newInstance\>_ | Swap an old instance with a new instance |
| _\-\-zkSvr \<ZookeeperServerAddress\>_ | Provide zookeeper address |

### REST Interface

The REST interface comes wit helix-admin-webapp package:

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.6.2-incubating
./build
cd helix-admin-webapp/target/helix-admin-webapp-pkg/bin
chmod +x *.sh
./run-rest-admin.sh --zkSvr <zookeeperAddress> --port <port> // make sure ZooKeeper is running
```

#### URL and support methods

* _/clusters_
    * List all clusters

    ```
    curl http://localhost:8100/clusters
    ```

    * Add a cluster

    ```
    curl -d 'jsonParameters={"command":"addCluster","clusterName":"MyCluster"}' -H "Content-Type: application/json" http://localhost:8100/clusters
    ```

* _/clusters/{clusterName}_
    * List cluster information

    ```
    curl http://localhost:8100/clusters/MyCluster
    ```

    * Enable/disable a cluster in distributed controller mode

    ```
    curl -d 'jsonParameters={"command":"activateCluster","grandCluster":"MyControllerCluster","enabled":"true"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster
    ```

    * Remove a cluster

    ```
    curl -X DELETE http://localhost:8100/clusters/MyCluster
    ```

* _/clusters/{clusterName}/resourceGroups_
    * List all resources in a cluster

    ```
    curl http://localhost:8100/clusters/MyCluster/resourceGroups
    ```

    * Add a resource to cluster

    ```
    curl -d 'jsonParameters={"command":"addResource","resourceGroupName":"MyDB","partitions":"8","stateModelDefRef":"MasterSlave" }' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/resourceGroups
    ```

* _/clusters/{clusterName}/resourceGroups/{resourceName}_
    * List resource information

    ```
    curl http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB
    ```

    * Drop a resource

    ```
    curl -X DELETE http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB
    ```

    * Reset all erroneous partitions of a resource

    ```
    curl -d 'jsonParameters={"command":"resetResource"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB
    ```

* _/clusters/{clusterName}/resourceGroups/{resourceName}/idealState_
    * Rebalance a resource

    ```
    curl -d 'jsonParameters={"command":"rebalance","replicas":"3"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB/idealState
    ```

    * Add an ideal state

    ```
    echo jsonParameters={
    "command":"addIdealState"
       }&newIdealState={
      "id" : "MyDB",
      "simpleFields" : {
        "IDEAL_STATE_MODE" : "AUTO",
        "NUM_PARTITIONS" : "8",
        "REBALANCE_MODE" : "SEMI_AUTO",
        "REPLICAS" : "0",
        "STATE_MODEL_DEF_REF" : "MasterSlave",
        "STATE_MODEL_FACTORY_NAME" : "DEFAULT"
      },
      "listFields" : {
      },
      "mapFields" : {
        "MyDB_0" : {
          "localhost_1001" : "MASTER",
          "localhost_1002" : "SLAVE"
        }
      }
    }
    > newIdealState.json
    curl -d @'./newIdealState.json' -H 'Content-Type: application/json' http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB/idealState
    ```

    * Add resource property

    ```
    curl -d 'jsonParameters={"command":"addResourceProperty","REBALANCE_TIMER_PERIOD":"500"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB/idealState
    ```

* _/clusters/{clusterName}/resourceGroups/{resourceName}/externalView_
    * Show resource external view

    ```
    curl http://localhost:8100/clusters/MyCluster/resourceGroups/MyDB/externalView
    ```
* _/clusters/{clusterName}/instances_
    * List all instances

    ```
    curl http://localhost:8100/clusters/MyCluster/instances
    ```

    * Add an instance

    ```
    curl -d 'jsonParameters={"command":"addInstance","instanceNames":"localhost_1001"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances
    ```

    * Swap an instance

    ```
    curl -d 'jsonParameters={"command":"swapInstance","oldInstance":"localhost_1001", "newInstance":"localhost_1002"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances
    ```
* _/clusters/{clusterName}/instances/{instanceName}_
    * Show instance information

    ```
    curl http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

    * Enable/disable an instance

    ```
    curl -d 'jsonParameters={"command":"enableInstance","enabled":"false"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

    * Drop an instance

    ```
    curl -X DELETE http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

    * Disable/enable partitions on an instance

    ```
    curl -d 'jsonParameters={"command":"enablePartition","resource": "MyDB","partition":"MyDB_0",  "enabled" : "false"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

    * Reset an erroneous partition on an instance

    ```
    curl -d 'jsonParameters={"command":"resetPartition","resource": "MyDB","partition":"MyDB_0"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

    * Reset all erroneous partitions on an instance

    ```
    curl -d 'jsonParameters={"command":"resetInstance"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/instances/localhost_1001
    ```

* _/clusters/{clusterName}/configs_
    * Get user cluster level config

    ```
    curl http://localhost:8100/clusters/MyCluster/configs/cluster
    ```

    * Set user cluster level config

    ```
    curl -d 'jsonParameters={"command":"setConfig","configs":"key1=value1,key2=value2"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/configs/cluster
    ```

    * Remove user cluster level config

    ```
    curl -d 'jsonParameters={"command":"removeConfig","configs":"key1,key2"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/configs/cluster
    ```

    * Get/set/remove user participant level config

    ```
    curl -d 'jsonParameters={"command":"setConfig","configs":"key1=value1,key2=value2"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/configs/participant/localhost_1001
    ```

    * Get/set/remove resource level config

    ```
    curl -d 'jsonParameters={"command":"setConfig","configs":"key1=value1,key2=value2"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/configs/resource/MyDB
    ```

* _/clusters/{clusterName}/controller_
    * Show controller information

    ```
    curl http://localhost:8100/clusters/MyCluster/Controller
    ```

    * Enable/disable cluster

    ```
    curl -d 'jsonParameters={"command":"enableCluster","enabled":"false"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/Controller
    ```

* _/zkPath/{path}_
    * Get information for zookeeper path

    ```
    curl http://localhost:8100/zkPath/MyCluster
    ```

* _/clusters/{clusterName}/StateModelDefs_
    * Show all state model definitions

    ```
    curl http://localhost:8100/clusters/MyCluster/StateModelDefs
    ```

    * Add a state mdoel definition

    ```
    echo jsonParameters={
      "command":"addStateModelDef"
    }&newStateModelDef={
      "id" : "OnlineOffline",
      "simpleFields" : {
        "INITIAL_STATE" : "OFFLINE"
      },
      "listFields" : {
        "STATE_PRIORITY_LIST" : [ "ONLINE", "OFFLINE", "DROPPED" ],
        "STATE_TRANSITION_PRIORITYLIST" : [ "OFFLINE-ONLINE", "ONLINE-OFFLINE", "OFFLINE-DROPPED" ]
      },
      "mapFields" : {
        "DROPPED.meta" : {
          "count" : "-1"
        },
        "OFFLINE.meta" : {
          "count" : "-1"
        },
        "OFFLINE.next" : {
          "DROPPED" : "DROPPED",
          "ONLINE" : "ONLINE"
        },
        "ONLINE.meta" : {
          "count" : "R"
        },
        "ONLINE.next" : {
          "DROPPED" : "OFFLINE",
          "OFFLINE" : "OFFLINE"
        }
      }
    }
    > newStateModelDef.json
    curl -d @'./untitled.txt' -H 'Content-Type: application/json' http://localhost:8100/clusters/MyCluster/StateModelDefs
    ```

* _/clusters/{clusterName}/StateModelDefs/{stateModelDefName}_
    * Show a state model definition

    ```
    curl http://localhost:8100/clusters/MyCluster/StateModelDefs/OnlineOffline
    ```

* _/clusters/{clusterName}/constraints/{constraintType}_
    * Show all contraints

    ```
    curl http://localhost:8100/clusters/MyCluster/constraints/MESSAGE_CONSTRAINT
    ```

    * Set a contraint

    ```
    curl -d 'jsonParameters={"constraintAttributes":"RESOURCE=MyDB,CONSTRAINT_VALUE=1"}' -H "Content-Type: application/json" http://localhost:8100/clusters/MyCluster/constraints/MESSAGE_CONSTRAINT/MyConstraint
    ```

    * Remove a constraint

    ```
    curl -X DELETE http://localhost:8100/clusters/MyCluster/constraints/MESSAGE_CONSTRAINT/MyConstraint
    ```
