What is HELIX
--------------
Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a group of nodes(cluster). Helix provides the following features 

1. Automatic assignment of resource/partition to nodes
2. Node Failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic Addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic Load balancing and throttling of transitions 

-----

Overview
--------
Helix uses terms that are commonly used to describe distributed data system concepts. 

1. Cluster: A logical set of Instances that perform similar set of activities. 
2. Instance/Node: Instance is logical entity in a cluster. Node is the physical entity in the cluster. A node can have one or more logical Instances. 
Node and Instance are used interchangeably. Each Instance in the cluster is identified by a unique id. 
Most commonly used convention is hostname\_port where hostname is the FQDN(Fully qualified Domain Name) and port is the port where the service is hosted. 
3. Resource: A resource represents the logical entity hosted by the distributed system. It can be a database name, index or a task group name 
4. Partition: A resource is generally split into one or partitions.
5. Replica: Each partition can have one or more replicas
6. State: Each replicas can have state associated with it. For example: Master, Slave, Leader, Stand By, Offline, Online etc. 

To summarize, A resource (Database, Index or any task) in general is partitioned, replicated and distributed among the Instance/nodes in the cluster and each partition has a state associated with it. 

Helix manages the state of a resource by supporting a pluggable distributed state machine. One can define the state machine table along with the constraints for each state. 

Here are some common state models used

1. Master, Slave
2. Online, Offline
3. Leader, Standby.

For example in case of a MasterSlave state model one can specify the state machine as follows. The table says given a start state and end state what should be the next state. 
For example, if the current state is Offline and target state is master, the table says next state is Slave.  

<pre><code>
          OFFLINE  | SLAVE  |  MASTER  
         _____________________________
        |          |        |         |
OFFLINE |   N/A    | SLAVE  | SLAVE   |
        |__________|________|_________|
        |          |        |         |
SLAVE   |  OFFLINE |   N/A  | MASTER  |
        |__________|________|_________|
        |          |        |         |
MASTER  | SLAVE    | SLAVE  |   N/A   |
        |__________|________|_________|

</code></pre>

Helix also supports the ability to provide constraints on each state. For example in a MasterSlave state model with a replication factor of 3 one can say 

    MASTER:1 
    SLAVE:2

Helix will automatically maintain 1 Master and 2 Slaves by initiating appropriate state transitions on each instance in the cluster. 

Each transition results in a partition moving from its CURRENT state state to an NEW state. These transitions are triggered on changes in the cluster state like 

* Node start up
* Node soft and hard failures 
* Addition of resources
* Addition of nodes

In simple words, Helix is a distributed state machine with support for constraints on each state.

Helix framework can be used to build distributed, scalable, elastic and fault tolerant systems by configuring the distributed state machine and its constraints based on application requirement. Application has to provide the implementation for handling state transitions appropriately. Example 

Once the state machine and constraints are configured through Helix, application will have the provide implementation to handle the transitions appropriately.  

<pre><code>
MasterSlaveStateModel extends HelixStateModel {

  void onOfflineToSlave(Message m, NotificationContext context){
    print("Transitioning from Offline to Slave for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onSlaveToMaster(Message m, NotificationContext context){
    print("Transitioning from Slave to Master for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onMasterToSlave(Message m, NotificationContext context){
    print("Transitioning from Master to Slave for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
  void onSlaveToOffline(Message m, NotificationContext context){
    print("Transitioning from Slave to Offline for resource:"+ m.getResourceName() + " and partition:"+ m.getPartitionName());
  }
}
</code></pre>

Once state machine is configured, the framework allows one to 

* Dynamically add nodes to the cluster
* Automatically modify the topology(rebalance partitions) of the cluster  
* Dynamically add resources to the cluster
* Enable/disable partition/instances for software upgrade without impacting availability.

Helix uses Zookeeper for maintaining the cluster state and change notifications.

Why HELIX
-------------
The approach of using a distributed state machine with constraints on state and transitions has many benefits like

* Abstract cluster management aspects from the core functionality of distributed system. Using a state machine to describe the process flow improves the overall design of the system.
* Each node in DDS is not aware of the global state since they simply have to follow instructions of the controller. This significantly reduces the code complexity 
* Since goal of Controller is to satisfy state machine constraints at all times, scenerios like cluster startup, node failure, cluster expansion are solved in a similar way.

At LinkedIn, we have used Helix to manage [3 different distributed systems](https://github.com/linkedin/helix/wiki/UseCases)  

----------------

Additional Info
---------------

Documentation: [Home](https://github.com/linkedin/helix/wiki/Home)  
Sample App using Helix: [rabbitmq-consumer-group] (https://github.com/linkedin/helix/wiki/Sample_App)  
Quickstart guide: [How to build and run a mock example](https://github.com/linkedin/helix/wiki/Quickstart)  
Architecture: [Helix Architecture](https://github.com/linkedin/helix/wiki/Architecture)  
Features: [Helix Features](https://github.com/linkedin/helix/wiki/Features)  
ApiUsage: http://linkedin.github.com/helix/apidocs/
UseCases: [Helix LinkedIn Usecases](https://github.com/linkedin/helix/wiki/UseCases)  




   
