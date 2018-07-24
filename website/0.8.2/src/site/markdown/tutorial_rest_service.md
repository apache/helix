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
  <title>Tutorial - REST Service 2.0</title>
</head>



## [Helix Tutorial](./Tutorial.html): REST Service 2.0

New Helix REST service supported features:

* Expose all admin operations via restful API.
    * All of Helix admin operations, include these defined in HelixAdmin.java and ConfigAccessor.java, etc, are exposed via rest API.
* Support all task framework API via restful.Current task framework operations are supported from rest API too.
* More standard Restful API
    * Use the standard HTTP methods if possible, GET, POST, PUT, DELETE, instead of customized command as it today.
    * Customized command will be used if there is no corresponding HTTP methods, for example, rebalance a resource, disable an instance, etc.
* Make Helix restful service an separately deployable service.
* Enable Access/Audit log for all write access.

### Installation
The command line tool comes with helix-core package:

Get the command line tool:

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.8.2
./build
cd helix-rest/target/helix-rest-pkg/bin
chmod +x *.sh
```

Get help:

```
./run-rest-admin.sh --help
```

Start the REST server

```
./run-rest-admin.sh --port 1234 --zkSvr localhost:2121
```

### Helix REST 2.0 Endpoint

Helix REST 2.0 endpoint will start with /admin/v2 prefix, and the rest will mostly follow the current URL convention.  This allows us to support v2.0 endpoint at the same time with the current Helix web interface. Some sample v2.0 endpoints would look like the following:

```
curl -X GET http://localhost:12345/admin/v2/clusters
curl -X POST http://localhost:12345/admin/v2/clusters/myCluster
curl -X POST http://localhost:12345/admin/v2/clusters/myCluster?command=activate&supercluster=controler_cluster
curl http://localhost:12345/admin/v2/clusters/myCluster/resources/myResource/IdealState
```
### REST Endpoints and Supported Operations
#### Operations on Helix Cluster

* **"/clusters"**
    *  Represents all helix managed clusters connected to given zookeeper
    *  **GET** -- List all Helix managed clusters. Example: curl http://localhost:1234/admin/v2/clusters

    ```
    $curl http://localhost:1234/admin/v2/clusters
    {
      "clusters" : [ "cluster1", "cluster2", "cluster3"]
    }
    ```


* **"/clusters/{clusterName}"**
    * Represents a helix cluster with name {clusterName}
    * **GET** -- return the cluster info. Example: curl http://localhost:1234/admin/v2/clusters/myCluster

        ```
        $curl http://localhost:1234/admin/v2/clusters/myCluster
        {
          "id" : "myCluster",
          "paused" : true,
          "disabled" : true,
          "controller" : "helix.apache.org:1234",
          "instances" : [ "aaa.helix.apache.org:1234", "bbb.helix.apache.org:1234" ],
          "liveInstances" : ["aaa.helix.apache.org:1234"],
          "resources" : [ "resource1", "resource2", "resource3" ],
          "stateModelDefs" : [ "MasterSlave", "LeaderStandby", "OnlineOffline" ]
        }
        ```

    * **PUT** – create a new cluster with {clusterName}, it returns 200 if the cluster already exists. Example: curl -X PUT http://localhost:1234/admin/v2/clusters/myCluster
    * **DELETE** – delete this cluster.
      Example: curl -X DELETE http://localhost:1234/admin/v2/clusters/myCluster
    * **activate** -- Link this cluster to a Helix super (controller) cluster, i.e, add the cluster as a resource to the super cluster.
      Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster?command=activate&superCluster=myCluster
    * **expand** -- In the case that a set of new node is added in the cluster, use this command to balance the resources on the existing instances to new added instances.
      Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster?command=expand
    * **enable** – enable/resume the cluster.
      Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster?command=enable
    * **disable** – disable/pause the cluster.
      Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster?command=disable

* **"/clusters/{clusterName}/configs"**
    * Represents cluster level configs for cluster with {clusterName}
    * **GET**: get all configs.
    
    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/configs
    {
      "id" : "myCluster",
      "simpleFields" : {
        "PERSIST_BEST_POSSIBLE_ASSIGNMENT" : "true"
      },
      "listFields" : {
      },
      "mapFields" : {
      }
    }
    ```

    * **POST**: update or delete one/some config entries.  
    update -- Update the entries included in the input.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/configs?command=update -d '
    {
     "id" : "myCluster",
      "simpleFields" : {
        "PERSIST_BEST_POSSIBLE_ASSIGNMENT" : "true"
      },
      "listFields" : {
        "disabledPartition" : ["p1", "p2", "p3"]
      },
      "mapFields" : {
      }
    }'
    ```
  
      delete -- Remove the entries included in the input from current config.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/configs?command=update -d '
    {
      "id" : "myCluster",
      "simpleFields" : {
      },
      "listFields" : {
        "disabledPartition" : ["p1", "p3"]
      },
      "mapFields" : {
      }
    }'
    ```

* **"/clusters/{clusterName}/controller"**
    * Represents the controller for cluster {clusterName}.
    * **GET** – return controller information

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/controller
    {
      "id" : "myCluster",
      "controller" : "test.helix.apache.org:1234",
      "HELIX_VERSION":"0.8.2",
      "LIVE_INSTANCE":"16261@test.helix.apache.org:1234",
      "SESSION_ID":"35ab496aba54c99"
    }
    ```

* **"/clusters/{clusterName}/controller/errors"**
    * Represents error information for the controller of cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** – get all error information.
    * **DELETE** – clean up all error logs.


* **"/clusters/{clusterName}/controller/history"**
    * Represents the change history of leader controller of cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** – get the leader controller history.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/controller/history
    {
      "id" : "myCluster",
      "history" [
          "{DATE=2017-03-21-16:57:14, CONTROLLER=test1.helix.apache.org:1234, TIME=1490115434248}",
          "{DATE=2017-03-27-22:35:16, CONTROLLER=test3.helix.apache.org:1234, TIME=1490654116484}",
          "{DATE=2017-03-27-22:35:24, CONTROLLER=test2.helix.apache.org:1234, TIME=1490654124926}"
      ]
    }
    ```

* **/clusters/{clusterName}/controller/messages"**
    * Represents all uncompleted messages currently received by the controller of cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** – list all uncompleted messages received by the controller.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/controller/messages
    {
      "id" : "myCluster",
      "count" : 5,
      "messages" [
          "0b8df4f2-776c-4325-96e7-8fad07bd9048",
          "13a8c0af-b77e-4f5c-81a9-24fedb62cf58"
      ]
    }
    ```

* **"/clusters/{clusterName}/controller/messages/{messageId}"**
    * Represents the messages currently received by the controller of cluster {clusterName} with id {messageId}. This is new endpoint in v2.0.
    * **GET** - get the message with {messageId} received by the controller.
    * **DELETE** - delete the message with {messageId}


* **"/clusters/{clusterName}/statemodeldefs/"**
    * Represents all the state model definitions defined in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get all the state model definition in the cluster.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/statemodeldefs
    {
      "id" : "myCluster",
      "stateModelDefs" : [ "MasterSlave", "LeaderStandby", "OnlineOffline" ]
    }
    ```

* **"/clusters/{clusterName}/statemodeldefs/{statemodeldef}"**
    * Represents the state model definition {statemodeldef} defined in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get the state model definition

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/statemodeldefs/MasterSlave
    {
      "id" : "MasterSlave",
      "simpleFields" : {
        "INITIAL_STATE" : "OFFLINE"
      },
      "mapFields" : {
        "DROPPED.meta" : {
          "count" : "-1"
        },
        "ERROR.meta" : {
          "count" : "-1"
        },
        "ERROR.next" : {
          "DROPPED" : "DROPPED",
          "OFFLINE" : "OFFLINE"
        },
        "MASTER.meta" : {
          "count" : "1"
        },
        "MASTER.next" : {
          "SLAVE" : "SLAVE",
          "DROPPED" : "SLAVE",
          "OFFLINE" : "SLAVE"
        },
        "OFFLINE.meta" : {
          "count" : "-1"
        },
        "OFFLINE.next" : {
          "SLAVE" : "SLAVE",
          "MASTER" : "SLAVE",
          "DROPPED" : "DROPPED"
        },
        "SLAVE.meta" : {
          "count" : "R"
        },
        "SLAVE.next" : {
          "MASTER" : "MASTER",
          "DROPPED" : "OFFLINE",
          "OFFLINE" : "OFFLINE"
        }
      },
      "listFields" : {
        "STATE_PRIORITY_LIST" : [ "MASTER", "SLAVE", "OFFLINE", "DROPPED", "ERROR" ],
        "STATE_TRANSITION_PRIORITYLIST" : [ "MASTER-SLAVE", "SLAVE-MASTER", "OFFLINE-SLAVE", "SLAVE-OFFLINE", "OFFLINE-DROPPED" ]
      }
    }
    ```

    * **POST** - add a new state model definition with {statemodeldef}
    * **DELETE** - delete the state model definition


#### Helix "Resource" and its sub-resources

* **"/clusters/{clusterName}/resources"**
    * Represents all resources in a cluster.
    * **GET** - list all resources with their IdealStates and ExternViews.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/resources
    {
      "id" : "myCluster",
      "idealstates" : [ "idealstate1", "idealstate2", "idealstate3" ],
      "externalviews" : [ "idealstate1", "idealstate3" ]
    }
    ```

* **"/clusters/{clusterName}/resources/{resourceName}"**
    * Represents a resource in cluster {clusterName} with name {resourceName}
    * **GET** - get resource info

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/resources/resource1
    {
      "id" : "resource1",
      "resourceConfig" : {},
      "idealState" : {},
      "externalView" : {}
    }
    ```

    * **PUT** - add a resource with {resourceName}

    ```
    $curl -X PUT -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource -d '
    {
      "id":"myResource",
      "simpleFields":{
        "STATE_MODEL_FACTORY_NAME":"DEFAULT"
        ,"EXTERNAL_VIEW_DISABLED":"true"
        ,"NUM_PARTITIONS":"1"
        ,"REBALANCE_MODE":"TASK"
        ,"REPLICAS":"1"
        ,"IDEAL_STATE_MODE":"AUTO"
        ,"STATE_MODEL_DEF_REF":"Task"
        ,"REBALANCER_CLASS_NAME":"org.apache.helix.task.WorkflowRebalancer"
      }
    }'
    ```

    * **DELETE** - delete a resource. Example: curl -X DELETE http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource
    * **enable** enable the resource. Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource?command=enable
    * **disable** - disable the resource. Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource?command=disable
    * **rebalance** - rebalance the resource. Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource?command=rebalance

* **"/clusters/{clusterName}/resources/{resourceName}/idealState"**
    * Represents the ideal state of a resource with name {resourceName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get idealstate.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource/idealState
    {
      "id":"myResource"
      ,"simpleFields":{
        "IDEAL_STATE_MODE":"AUTO"
        ,"NUM_PARTITIONS":"2"
        ,"REBALANCE_MODE":"SEMI_AUTO"
        ,"REPLICAS":"2"
        ,"STATE_MODEL_DEF_REF":"MasterSlave"
      }
      ,"listFields":{
        "myResource_0":["host1", "host2"]
        ,"myResource_1":["host2", "host1"]
      }
      ,"mapFields":{
        "myResource_0":{
          "host1":"MASTER"
          ,"host2":"SLAVE"
        }
        ,"myResource_1":{
          "host1":"SLAVE"
          ,"host2":"MASTER"
        }
      }
    }
    ```

* **"/clusters/{clusterName}/resources/{resourceName}/externalView"**
    * Represents the external view of a resource with name {resourceName} in cluster {clusterName}
    * **GET** - get the externview

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource/externalView
    {
      "id":"myResource"
      ,"simpleFields":{
        "IDEAL_STATE_MODE":"AUTO"
        ,"NUM_PARTITIONS":"2"
        ,"REBALANCE_MODE":"SEMI_AUTO"
        ,"REPLICAS":"2"
        ,"STATE_MODEL_DEF_REF":"MasterSlave"
      }
      ,"listFields":{
        "myResource_0":["host1", "host2"]
        ,"myResource_1":["host2", "host1"]
      }
      ,"mapFields":{
        "myResource_0":{
          "host1":"MASTER"
          ,"host2":"OFFLINE"
        }
        ,"myResource_1":{
          "host1":"SLAVE"
          ,"host2":"MASTER"
        }
      }
    }
    ```

* **"/clusters/{clusterName}/resources/{resourceName}/configs"**
    * Represents resource level of configs for resource with name {resourceName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get resource configs.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/resources/myResource/configs
    {
      "id":"myDB"
      "UserDefinedProperty" : "property"
    }
    ```

#### Helix Instance and its sub-resources

* **"/clusters/{clusterName}/instances"**
    * Represents all instances in a cluster {clusterName}
    * **GET** - list all instances in this cluster.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances
    {
      "id" : "myCluster",
      "instances" : [ "host1", "host2", "host3", "host4"],
      "online" : ["host1", "host4"],
      "disabled" : ["host2"]
    }
    ```

    * **POST** - enable/disable instances.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/command=enable -d
    {
      "instances" : [ "host1", "host3" ]
    }
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/command=disable -d
    {
      "instances" : [ "host2", "host4" ]
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}"**
    * Represents a instance in cluster {clusterName} with name {instanceName}
    * **GET** - get instance information.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234
    {
      "id" : "host_1234",
      "configs" : {
        "HELIX_ENABLED" : "true",
        "HELIX_HOST" : "host",
        "HELIX_PORT" : "1234",
        "HELIX_DISABLED_PARTITION" : [ ]
      }
      "liveInstance" : {
        "HELIX_VERSION":"0.6.6.3",
        "LIVE_INSTANCE":"4526@host",
        "SESSION_ID":"359619c2d7efc14"
      }
    }
    ```

    * **PUT** - add a new instance with {instanceName}

    ```
    $curl -X PUT -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234 -d '
    {
      "id" : "host_1234",
      "simpleFields" : {
        "HELIX_ENABLED" : "true",
        "HELIX_HOST" : "host",
        "HELIX_PORT" : "1234",
      }
    }'
    ```
  
    There's one important restriction for this operation: the {instanceName} should match exactly HELIX_HOST + "_" + HELIX_PORT. For example, if host is localhost, and port is 1234, the instance name should be localhost_1234. Otherwise, the response won't contain any error but the configurations are not able to be filled in.

    * **DELETE** - delete the instance. Example: curl -X DELETE http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234
    * **enable** - enable the instance. Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234?command=enable
    * **disable** - disable the instance. Example: curl -X POST http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234?command=disable

    * **addInstanceTag** -  add tags to this instance.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234?command=addInstanceTag -d '
    {
      "id" : "host_1234",
      "instanceTags" : [ "tag_1", "tag_2, "tag_3" ]
    }'
    ```

    * **removeInstanceTag** - remove a tag from this instance.

    ```
    $curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234?command=removeInstanceTag -d '
    {
      "id" : "host_1234",
      "instanceTags" : [ "tag_1", "tag_2, "tag_3" ]
    }'
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/resources"**
    * Represents all resources and their partitions locating on the instance in cluster {clusterName} with name {instanceName}. This is new endpoint in v2.0.
    * **GET** - return all resources that have partitions in the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/resources
    {
      "id" : "host_1234",
      "resources" [ "myResource1", "myResource2", "myResource3"]
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/resources/{resource}"**
    * Represents all partitions of the {resource}  locating on the instance in cluster {clusterName} with name {instanceName}. This is new endpoint in v2.0.
    * **GET** - return all partitions of the resource in the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/localhost_1234/resources/myResource1
    {
      "id":"myResource1"
      ,"simpleFields":{
        "STATE_MODEL_DEF":"MasterSlave"
        ,"STATE_MODEL_FACTORY_NAME":"DEFAULT"
        ,"BUCKET_SIZE":"0"
        ,"SESSION_ID":"359619c2d7f109b"
      }
      ,"listFields":{
      }
      ,"mapFields":{
        "myResource1_2":{
          "CURRENT_STATE":"SLAVE"
          ,"INFO":""
        }
        ,"myResource1_3":{
          "CURRENT_STATE":"MASTER"
          ,"INFO":""
        }
        ,"myResource1_0":{
          "CURRENT_STATE":"MASTER"
          ,"INFO":""
        }
        ,"myResource1_1":{
          "CURRENT_STATE":"SLAVE"
          ,"INFO":""
        }
      }
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/configs"**
    * Represents instance configs in cluster {clusterName} with name {instanceName}. This is new endpoint in v2.0.
    * **GET** - return configs for the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/configs 
    {
      "id":"host_1234"
      "configs" : {
        "HELIX_ENABLED" : "true",
        "HELIX_HOST" : "host"
        "HELIX_PORT" : "1234",
        "HELIX_DISABLED_PARTITION" : [ ]
    }
    ```

    * **PUT** - PLEASE NOTE THAT THIS PUT IS FULLY OVERRIDE THE INSTANCE CONFIG

    ```
    $curl -X PUT -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/configs
    {
      "id":"host_1234"
      "configs" : {
        "HELIX_ENABLED" : "true",
        "HELIX_HOST" : "host"
        "HELIX_PORT" : "1234",
        "HELIX_DISABLED_PARTITION" : [ ]
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/errors"**
    * List all the mapping of sessionId to partitions of resources. This is new endpoint in v2.0.
    * **GET** - get mapping

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/errors
    {
       "id":"host_1234"
       "errors":{
            "35sfgewngwese":{
                "resource1":["p1","p2","p5"],
                "resource2":["p2","p7"]
             }
        }
    }
    ```

    * **DELETE** - clean up all error information from Helix.

* **"/clusters/{clusterName}/instances/{instanceName}/errors/{sessionId}/{resourceName}/{partitionName}"**
    * Represents error information for the partition {partitionName} of the resource {resourceName} under session {sessionId} in instance with {instanceName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get all error information.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/errors/35sfgewngwese/resource1/p1
    {
      "id":"35sfgewngwese_resource1"
      ,"simpleFields":{
      }
      ,"listFields":{
      }
      ,"mapFields":{
        "HELIX_ERROR     20170521-070822.000561 STATE_TRANSITION b819a34d-41b5-4b42-b497-1577501eeecb":{
          "AdditionalInfo":"Exception while executing a state transition task ..."
          ,"MSG_ID":"4af79e51-5f83-4892-a271-cfadacb0906f"
          ,"Message state":"READ"
        }
      }
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/history"**
    * Represents instance session change history for the instance with {instanceName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - get the instance change history.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/history
    {
      "id": "host_1234",
      "LAST_OFFLINE_TIME": "183948792",
      "HISTORY": [
        "{DATE=2017-03-02T19:25:18:915, SESSION=459014c82ef3f5b, TIME=1488482718915}",
        "{DATE=2017-03-10T22:24:53:246, SESSION=15982390e5d5c91, TIME=1489184693246}",
        "{DATE=2017-03-11T02:03:52:776, SESSION=15982390e5d5d85, TIME=1489197832776}",
        "{DATE=2017-03-13T18:15:00:778, SESSION=15982390e5d678d, TIME=1489428900778}",
        "{DATE=2017-03-21T02:47:57:281, SESSION=459014c82effa82, TIME=1490064477281}",
        "{DATE=2017-03-27T14:51:06:802, SESSION=459014c82f01a07, TIME=1490626266802}",
        "{DATE=2017-03-30T00:05:08:321, SESSION=5590151804e2c78, TIME=1490832308321}",
        "{DATE=2017-03-30T01:17:34:339, SESSION=2591d53b0421864, TIME=1490836654339}",
        "{DATE=2017-03-30T17:31:09:880, SESSION=2591d53b0421b2a, TIME=1490895069880}",
        "{DATE=2017-03-30T18:05:38:220, SESSION=359619c2d7f109b, TIME=1490897138220}"
      ]
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/messages"**
    * Represents all uncompleted messages currently received by the instance. This is new endpoint in v2.0.
    * **GET** - list all uncompleted messages received by the controller.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/messages
    {
      "id": "host_1234",
      "new_messages": ["0b8df4f2-776c-4325-96e7-8fad07bd9048", "13a8c0af-b77e-4f5c-81a9-24fedb62cf58"],
      "read_messages": ["19887b07-e9b8-4fa6-8369-64146226c454"]
      "total_message_count" : 100,
      "read_message_count" : 50
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/messages/{messageId}**
    * Represents the messages currently received by by the instance with message given message id. This is new endpoint in v2.0.
    * **GET** - get the message content with {messageId} received by the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/localhost_1234/messages/0b8df4f2-776c-4325-96e7-8fad07bd9048
    {
      "id": "0b8df4f2-776c-4325-96e7-8fad07bd9048",
      "CREATE_TIMESTAMP":"1489997469400",
      "ClusterEventName":"messageChange",
      "FROM_STATE":"OFFLINE",
      "MSG_ID":"0b8df4f2-776c-4325-96e7-8fad07bd9048",
      "MSG_STATE":"new",
      "MSG_TYPE":"STATE_TRANSITION",
      "PARTITION_NAME":"Resource1_243",
      "RESOURCE_NAME":"Resource1",
      "SRC_NAME":"controller_1234",
      "SRC_SESSION_ID":"15982390e5d5a76",
      "STATE_MODEL_DEF":"LeaderStandby",
      "STATE_MODEL_FACTORY_NAME":"myFactory",
      "TGT_NAME":"host_1234",
      "TGT_SESSION_ID":"459014c82efed9b",
      "TO_STATE":"DROPPED"
    }
    ```

    * **DELETE** - delete the message with {messageId}. Example: $curl -X DELETE http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/messages/0b8df4f2-776c-4325-96e7-8fad07bd9048

* **"/clusters/{clusterName}/instances/{instanceName}/healthreports"**
    * Represents all health reports in the instance in cluster {clusterName} with name {instanceName}. This is new endpoint in v2.0.
    * **GET** - return the name of health reports collected from the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/healthreports
    {
      "id" : "host_1234",
      "healthreports" [ "report1", "report2", "report3" ]
    }
    ```

* **"/clusters/{clusterName}/instances/{instanceName}/healthreports/{reportName}"**
    * Represents the health report with {reportName} in the instance in cluster {clusterName} with name {instanceName}. This is new endpoint in v2.0.
    * **GET** - return the content of health report collected from the instance.

    ```
    $curl http://localhost:1234/admin/v2/clusters/myCluster/instances/host_1234/healthreports/ClusterStateStats
    {
      "id":"ClusterStateStats"
      ,"simpleFields":{
        "CREATE_TIMESTAMP":"1466753504476"
        ,"TimeStamp":"1466753504476"
      }
      ,"listFields":{
      }
      ,"mapFields":{
        "UserDefinedData":{
          "Data1":"0"
          ,"Data2":"0.0"
        }
      }
    }
    ```


#### Helix Workflow and its sub-resources

* **"/clusters/{clusterName}/workflows"**
    * Represents all workflows in cluster {clusterName}
    * **GET** - list all workflows in this cluster. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows

    ```
    {
      "Workflows" : [ "Workflow1", "Workflow2" ]
    }
    ```

* **"/clusters/{clusterName}/workflows/{workflowName}"**
    * Represents workflow with name {workflowName} in cluster {clusterName}
    * **GET** - return workflow information. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1

    ```
    {
       "id" : "Workflow1",
       "WorkflowConfig" : {
           "Expiry" : "43200000",
           "FailureThreshold" : "0",
           "IsJobQueue" : "true",
           "LAST_PURGE_TIME" : "1490820801831",
           "LAST_SCHEDULED_WORKFLOW" : "Workflow1_20170329T000000",
           "ParallelJobs" : "1",
           "RecurrenceInterval" : "1",
           "RecurrenceUnit" : "DAYS",
           "START_TIME" : "1482176880535",
           "STATE" : "STOPPED",
           "StartTime" : "12-19-2016 00:00:00",
           "TargetState" : "START",
           "Terminable" : "false",
           "capacity" : "500"
        },
       "WorkflowContext" : {
           "JOB_STATES": {
             "Job1": "COMPLETED",
             "Job2": "COMPLETED"
           },
           "StartTime": {
             "Job1": "1490741582339",
             "Job2": "1490741580204"
           },
           "FINISH_TIME": "1490741659135",
           "START_TIME": "1490741580196",
           "STATE": "COMPLETED"
       },
       "Jobs" : ["Job1","Job2","Job3"],
       "ParentJobs" : {
            "Job1":["Job2", "Job3"],
            "Job2":["Job3"]
       }
    }
    ```

    * **PUT** - create a workflow with {workflowName}. Example : curl -X PUT -H "Content-Type: application/json" -d [WorkflowExample.json](./WorkflowExample.json) http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1
    * **DELETE** - delete the workflow. Example : curl -X DELETE http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1
    * **start** - start the workflow. Example : curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1?command=start
    * **stop** - pause the workflow. Example : curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1?command=stop
    * **resume** - resume the workflow. Example : curl -X POST -H "Content-Type: application/json" http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1?command=resume
    * **cleanup** - cleanup all expired jobs in the workflow, this operation is only allowed if the workflow is a JobQueue. Example : curl -X POST -H "Content-Type: application/json"  http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1?command=clean

* **"/clusters/{clusterName}/workflows/{workflowName}/configs"**
    * Represents workflow config with name {workflowName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - return workflow configs. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/configs

    ```
    {
        "id": "Workflow1",
        "Expiry" : "43200000",
        "FailureThreshold" : "0",
        "IsJobQueue" : "true",
        "START_TIME" : "1482176880535",
        "StartTime" : "12-19-2016 00:00:00",
        "TargetState" : "START",
        "Terminable" : "false",
        "capacity" : "500"
    }
    ```

* **"/clusters/{clusterName}/workflows/{workflowName}/context"**
    * Represents workflow runtime information with name {workflowName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - return workflow runtime information. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/context

    ```
    {
        "id": "WorkflowContext",
        "JOB_STATES": {
             "Job1": "COMPLETED",
             "Job2": "COMPLETED"
         },
         "StartTime": {
             "Job1": "1490741582339",
             "Job2": "1490741580204"
         },
         "FINISH_TIME": "1490741659135",
         "START_TIME": "1490741580196",
         "STATE": "COMPLETED"
    }
    ```


#### Helix Job and its sub-resources

* **"/clusters/{clusterName}/workflows/{workflowName}/jobs"**
    * Represents all jobs in workflow {workflowName} in cluster {clusterName}
    * **GET** return all job names in this workflow. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs

    ```
    {
        "id":"Jobs"
        "Jobs":["Job1","Job2","Job3"]
    }
    ```

* **"/clusters/{clusterName}/workflows/{workflowName}/jobs/{jobName}"**
    * Represents job with {jobName} within {workflowName} in cluster {clusterName}
    * **GET** return job information. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs/Job1

    ```
    {
        "id":"Job1"
        "JobConfig":{
            "WorkflowID":"Workflow1",
            "IgnoreDependentJobFailure":"false",
            "MaxForcedReassignmentsPerTask":"3"
        },
        "JobContext":{
    	"START_TIME":"1491005863291",
            "FINISH_TIME":"1491005902612",
            "Tasks":[
                 {
                     "id":"0",
                     "ASSIGNED_PARTICIPANT":"P1",
                     "FINISH_TIME":"1491005898905"
                     "INFO":""
                     "NUM_ATTEMPTS":"1"
                     "START_TIME":"1491005863307"
                     "STATE":"COMPLETED"
                     "TARGET":"DB_0"
                 },
                 {
                     "id":"1",
                     "ASSIGNED_PARTICIPANT":"P5",
                     "FINISH_TIME":"1491005895443"
                     "INFO":""
                     "NUM_ATTEMPTS":"1"
                     "START_TIME":"1491005863307"
                     "STATE":"COMPLETED"
                     "TARGET":"DB_1"
                 }
             ]
         }
    }
    ```

    * **PUT** - insert a job with {jobName} into the workflow, this operation is only allowed if the workflow is a JobQueue.  
      Example : curl -X PUT -H "Content-Type: application/json" -d [JobExample.json](./JobExample.json) http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs/Job1
    * **DELETE** - delete the job from the workflow, this operation is only allowed if the workflow is a JobQueue.  
      Example : curl -X DELETE http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs/Job1

* **"/clusters/{clusterName}/workflows/{workflowName}/jobs/{jobName}/configs"**
    * Represents job config for {jobName} within workflow {workflowName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - return job config. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs/Job1/configs

    ```
    {
      "id":"JobConfig"
      "WorkflowID":"Workflow1",
      "IgnoreDependentJobFailure":"false",
      "MaxForcedReassignmentsPerTask":"3"
    }
    ```

* **"/clusters/{clusterName}/workflows/{workflowName}/jobs/{jobName}/context"**
    * Represents job runtime information with {jobName} in {workflowName} in cluster {clusterName}. This is new endpoint in v2.0.
    * **GET** - return job runtime information. Example : curl http://localhost:1234/admin/v2/clusters/TestCluster/workflows/Workflow1/jobs/Job1/context

    ```
    {
       "id":"JobContext":
       "START_TIME":"1491005863291",
       "FINISH_TIME":"1491005902612",
       "Tasks":[
                 {
                     "id":"0",
                     "ASSIGNED_PARTICIPANT":"P1",
                     "FINISH_TIME":"1491005898905"
                     "INFO":""
                     "NUM_ATTEMPTS":"1"
                     "START_TIME":"1491005863307"
                     "STATE":"COMPLETED"
                     "TARGET":"DB_0"
                 },
                 {
                     "id":"1",
                     "ASSIGNED_PARTICIPANT":"P5",
                     "FINISH_TIME":"1491005895443"
                     "INFO":""
                     "NUM_ATTEMPTS":"1"
                     "START_TIME":"1491005863307"
                     "STATE":"COMPLETED"
                     "TARGET":"DB_1"
                 }
       ]
    }
    ```
