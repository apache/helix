WHAT IS HELIX
--------------
Helix is a generic cluster management framework used for automatic management of partitioned, replicated and distributed resources hosted on a group of nodes( cluster). Helix provides the following features 

1. Automatic assignment of resource/partition to nodes
2. Node Failure detection and recovery
3. Dynamic addition of Resources 
4. Dynamic Addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource via state transitions
6. Automatic Load balancing and throttling of transitions 

-----

A resource (for eg a database, lucene index or any task) in general is partitioned, replicated and distributed among the nodes in the cluster. 

Each partition of a resource can have a state associated with it. Here are some common state models used

1. Master, Slave
2. Online, Offline
3. Leader, Standby.

Helix manages the state of a resource by supporting a pluggable distributed state machine. One can define the state machine table along with the constraints for each state. For example in case of a MasterSlave state model one can specify the state machine as

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

Helix will automatically try to maintain 1 Master and 2 Slaves by initiating appropriate state transitions in the cluster. 

Each transition results in a partition moving from its CURRENT state state to an NEW state. These transitions are triggered on changes in the cluster state like 

* Node start up
* Node soft and hard failures 
* Addition of resources
* Addition of nodes

---------


With these features, Helix framework can be used to build distributed, scalable, elastic and fault tolerant systems by configuring the distributed state machine and its constraints based on application requirement. Application has to provide the implementation for handling state transitions appropriately. Example 

<pre><code>
MasterSlaveStateModel extends HelixStateModel {

  void onOfflineToSlave(Message m, NotificationContext context){
    print("Transitioning from Offline to Slave for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onSlaveToMaster(Message m, NotificationContext context){
    print("Transitioning from Slave to Master for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onMasterToSlave(Message m, NotificationContext context){
    print("Transitioning from Master to Slave for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }
  void onSlaveToOffline(Message m, NotificationContext context){
    print("Transitioning from Slave to Offline for database:"+ m.resourceGroup + " and partition:"+ m.resourceKey);
  }

}

</code></pre>

The only thing users have to now worry about is handling the transitions appropriately.

Helix uses Zookeeper for maintaining the cluster state and change notification.

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



