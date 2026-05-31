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
  <title>Tutorial - Distributed Lock</title>
</head>

## [Helix Tutorial](./Tutorial.html): Distributed Lock

In a distributed system, there are many cases that we need a mechanism to make sure different processes can cooperate correctly. For example, if two processes that run on different machines, or different networks, or even different data centers would like to work on the same resource, we must have a mutually exclusive lock to ensure that their operations do not step on each other. Only when a process acquires the lock, it can perform the operation. 

Since Helix is built on top of ZooKeeper, which is a widely used framework to manage coordination across a cluster of machines, a natural solution for implementing a distributed lock is to leverage ZooKeeper. Specifically, we can represent the lock itself as a znode. Currently, many distributed systems use Apache Helix as a generic cluster management framework to automatically manage resources hosted on a cluster of nodes. Implementing a distributed lock in Helix would allow Helix's existing customers to use a reliable lock with little overhead.

### Helix Lock Interface
Helix provides a generic lock interface that can be implemented with different kinds of locks. Several key functions are defined in the interface.

```
/**
 * Generic interface for Helix distributed lock
 */
public interface DistributedLock {
  /**
   * Blocking call to acquire a lock if it is free at the time of request
   * @return true if the lock was successfully acquired,
   * false if the lock could not be acquired
   */
  boolean tryLock();

  /**
   * Blocking call to unlock a lock
   * @return true if the lock was successfully released or if the locked is not currently locked,
   * false if the lock is not locked by the user or the unlock operation failed
   */
  boolean unlock();

  /**
   * Retrieve the information of the current lock on the resource this lock object specifies, e.g. lock timeout, lock message, etc.
   * @return lock metadata information
   */
  LockInfo getCurrentLockInfo();

  /**
   * If the user is current lock owner of the resource
   * @return true if the user is the lock owner,
   * false if the user is not the lock owner or the lock doesn't have a owner
   */
  boolean isCurrentOwner();

  /**
   * Call this method to close the lock's zookeeper connection
   * The lock has to be unlocked or expired before this method can be called
   */
  void close();
}
```

Currently, Helix supports two kinds of locks. One is basic lock, and the other is priority lock. We will describe their definition and usage as follows.

### Helix Basic Lock
#### Features
* Non-blocking lock. It means every request to acquire the lock will immediately get a result (success or failure) instead of waiting until the lock is successfully acquired.

* Timeout support. A client needs to input the timeout when it tries to acquire the lock. We use lazy timeout for the lock. It means even after the timeout, if no other client tries to get the lock, the client can still keep the lock, if other client tries to acquire the lock, and finds the previous client already timed out, the previous client will automatically lose the lock.
  
* Lock message support. A client can input the reason for the lock (as a string) when it tries to acquire the lock. This enables future lock operation, when fails, can know the reason from the lock message of the current lock. 
  
* Java API support for clients to use the lock.
  
* No notification support. When a client loses its lock after the timeout or due to some urgent maintenance work who does not check the lock, there is no notification sent out to the previous lock owner.

#### How to Use Helix Basic Lock
The implementation of Helix lock is called "ZkHelixNonblockingLock". It has two different constructors as shown below. One constructor takes clusterName, and HelixConfigScope as inputs. HelixConfigScope can be "cluster", "participant", or "resource". The other constructor takes a lockPath string as input.
``` 
  /**
   * Initialize the lock with Helix scope.
   */
  public ZKHelixNonblockingLock(String clusterName, HelixConfigScope scope, String zkAddress,
      Long timeout, String lockMsg) 
 
  /**
   * Initialize the lock with lock path under zookeeper.
   */
  public ZKHelixNonblockingLock(String lockPath, String zkAddress, Long timeout, String lockMsg) 
```

The client can use the lock in two different ways. If the client would like to use the lock for some specific Helix resource, it can use the first constructor which requires clusterName and HelixConfigScope. We show an example below on how to use Helix scope. Or the client may choose to use the lock for generic purpose, basically on any resource they have. In this case, the client needs to provide the lock path, which is a string, and it represents where the lock should exist under zookeeper.
```
    List<String> pathKeys = new ArrayList<>();
    pathKeys.add(clusterName);
    HelixLockScope participantScope = new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER, pathKeys);
```

Note that if users have onboarded to ZooScalability that has multiple realms, they should instead use the builder instead of the above constructors.


### Helix Priority Lock
#### Features
* Priority support. A client with higher priority can override lock owned by a client with lower priority.
* Notification support. After the higher priority client overrides the lock owned by lower priority client, the low priority client will get notified. 
* Cleanup support. The low priority client will be given some time to do the cleanup and roll back the system to normal state when its lock is preempted. 

#### Priority Lock Definitions
There are a few concepts defined for priority lock. 

##### Definition 1: Priority of the lock. 
Any client that wants to acquire a lock needs to have a priority so that Helix knows what to do when there are multiple clients that want the same lock. We accept non-negative integers as meaningful priority representation. The lowest priority is 0. The larger the number, the higher the priority is. Same number denotes the same priority. We do not have an upper limit for the priority definition. However, we reserve the value of INT_MAX for emergency use by SREs, i.e., they may override all other priorities in case of urgent operations. If a client does not have a priority defined when it tries to acquire the lock, the default value 0 will be assigned to the client and thus the client is with lowest priority. 

##### Definition 2: Timeout of the lock. 
When a request for a lock is issued, there can be three types of timeout associated with the lock. We explain them respectively as follows.

* lease timeout: it is the user's requirement of how long the lease it needs. It denotes how long it takes to finish the work by the client that requires the locked resource. This is the same concept as the timeout in basic lock. This field must be set to a positive value. Otherwise the lock is meaningless.

* cleanup timeout: it defines how long it takes for a client to clean up the work done with the locked resource and bring back the resource to a clean state if the client is preempted by a higher priority client. If a user does not define this field, the clean up timeout will be set to the default value 0, meaning no cleanup work is needed. Once there is a higher priority client requested the lock, the lock will be immediately given to the high priority one after the notification is sent to the low priority client. 

* waiting timeout: it defines how long the client that requests the lock can wait for the lower priority client to finish the cleanup work. Specifically, if the work is urgent to get executed, a client should set a shorter waiting timeout. To the extreme, if users set the waiting timeout to be 0, the lock will be acquired immediately. If the waiting timeout is not set, it will be default as infinite, meaning that the high priority client will not get the lock until the low priority client finishes all the cleanup work. 

The three timeout are specific to the particular client that requests the lock and should be independent of each other. In short, the lease timeout includes all the time needed in normal operation (perform the work, clean up when work is completed, etc.); cleanup timeout is user's best estimate of time needed for reverting the system to previous stable state in case of preemption; waiting timeout shows how urgent the client needs to acquire the lock. Please note that it is possible that a client's waiting timeout is shorter than the other client's cleanup timeout. In this case, the client's waiting timeout will override the other client's cleanup timeout. We always honor the waiting timeout of the high priority client before cleanup timeout of the low priority client when there is a contention for the lock. 
Lock users should be responsible for setting up different timeout of their locks. However, it is possible that at the beginning they can only provide an estimation for the timeout which is not accurate enough. Either Helix or user code could implement some statistic logic to analyze the real execution or clean up times used. For example, Helix may be able to calculate how many times the workflow finishes the cleanup job in the designated cleanup timeout, and if not, what is the real cleanup time (in case that the waiting timeout is larger). Based on the results, users could tune their timeout to better handle lock preemption. 

##### Definition 3: Forcefulness of the lock
Forcefulness means whether a lock requested by a client is forceful or not in case of a preemption. If it is a forceful lock, when time is out, no matter whether the cleanup work of low priority client is finished or not, the high priority client will grab the lock from low priority client. However, if it is not a forceful lock, the high priority client will receive an exception, knowing that the low priority client has not finished its work yet. Then it is up to human beings to decide what is the next step. We could trigger an alert based on this kind of exception. Users can set up an approval workflow to act based on the alert. They may manually check the exception and the previous lock owner to make the decision on whether they will respect the previous owner's work or forcefully grab the lock. Or later with some experience on handling these exceptions, they can also make the process fully automatic. For example, with low priority client A and high priority client B, if in the case of exception thrown during preemption, user always chooses to forcefully grab the lock and assign the lock owner to B, we may have a rule set up for this condition which will automatically transfer the lock owner.

#### Priority Lock Workflow
To better visualize the above analysis, we draw the following diagram shows how a client may go through the possible scenarios.
![HelixPriorityLockWorkflow](./images/HelixPriorityLockWorkflow.jpeg)

#### Priority Lock Usage
##### Lock information Definition
```
  public enum LockInfoAttribute {
    OWNER,
    MESSAGE,
    TIMEOUT,
    PRIORITY,
    WAITING_TIMEOUT,
    CLEANUP_TIMEOUT,
    REQUESTOR_ID,
    REQUESTOR_PRIORITY,
    REQUESTOR_WAITING_TIMEOUT,
    REQUESTING_TIMESTAMP
  }
```

##### Priority Lock Constructor
```
  /**
   * Internal construction of the lock with user provided information, e.g., lock path under
   * zookeeper, etc.
   * @param lockPath the path of the lock under Zookeeper
   * @param leaseTimeout the leasing timeout period of the lock
   * @param lockMsg the reason for having this lock
   * @param userId a universal unique userId for lock owner identity
   * @param priority the priority of the lock
   * @param waitingTimeout the waiting timeout period of the lock when the tryLock request is issued
   * @param cleanupTimeout the time period needed to finish the cleanup work by the lock when it
   *                      is preempted
   * @param isForceful whether the lock is a forceful one. This determines the behavior when the
   *                   lock encountered an exception during preempting lower priority lock
   * @param lockListener the listener associated to the lock
   * @param baseDataAccessor baseDataAccessor instance to do I/O against ZK with
   */
  private ZKDistributedNonblockingLock(String lockPath, Long leaseTimeout, String lockMsg,
      String userId, int priority, long waitingTimeout, long cleanupTimeout, boolean isForceful,
      LockListener lockListener, BaseDataAccessor<ZNRecord> baseDataAccessor) 
```

##### Priority Lock Usage Example:
```
    ZKDistributedNonblockingLock.Builder lockBuilder = new ZKDistributedNonblockingLock.Builder();
    lockBuilder.setLockScope(new HelixLockScope(HelixLockScope.LockScopeProperty.CLUSTER, pathKeys)).setZkAddress(ZK_ADDR).setTimeout(3600000L)
        .setLockMsg("test lock").setUserId("test Id").setPriority(0).setWaitingTimeout(1000)
        .setCleanupTimeout(2000).setIsForceful(false)
        .setLockListener(new LockListener() {@Override public void onCleanupNotification() {});
    ZKDistributedNonblockingLock testLock = lockBuilder.build();
```
