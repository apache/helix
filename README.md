Helix is a generic cluster manager used for automatic management of partitioned, replicated and distributed resources hosted on a cluster of machines. Helix provides the following features 

1. Resource assignment to nodes
2. Dynamic addition of Resources 
3. Node Failure detection and recovery
4. Addition of nodes to the cluster
5. Pluggable distributed state machine to manage the state of a resource

A resource (for eg a database, lucene index or any task) in general is partitioned, replicated and distributed among the nodes in the cluster. Apart from managing the assignment of resource to Nodes, each partition of a resource can have a state associated with it ( Master, Slave ).

Helix manages the state of a resource by supporting a pluggable distributed state machine. One can define the state machine table along with the constraints for each state. For example if it is MasterSlave system one can specify the state machine as
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

Constraint on the states can be specified as MASTER:1 SLAVE:2. Helix will automatically try to maintain 1 Master and 2 Slaves by initiating appropriate state transitions in the cluster. These transitions are triggered on changes in the cluster state like 
 * Node startsup
 * Node dies 
 * New resource is added
 * New nodes are added

With these features Helix can be used to build distributed, scalable, elastic and fault tolerant systems by configuring application state machine. All the application needs to implement is the handling state transitions appropriately. Example 

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









