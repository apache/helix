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
  <title>Tutorial - Messaging</title>
</head>

## [Helix Tutorial](./Tutorial.html): Messaging

In this chapter, we\'ll learn about messaging, a convenient feature in Helix for sending messages between nodes of a cluster.  This is an interesting feature that is quite useful in practice. It is common that nodes in a distributed system require a mechanism to interact with each other.

### Example: Bootstrapping a Replica

Consider a search system  where the index replica starts up and it does not have an index. A typical solution is to get the index from a common location, or to copy the index from another replica.

Helix provides a messaging API for intra-cluster communication between nodes in the system.  This API provides a mechanism to specify the message recipient in terms of resource, partition, and state rather than specifying hostnames.  Helix ensures that the message is delivered to all of the required recipients. In this particular use case, the instance can specify the recipient criteria as all replicas of the desired partition to bootstrap.
Since Helix is aware of the global state of the system, it can send the message to the appropriate nodes. Once the nodes respond, Helix provides the bootstrapping replica with all the responses.

This is a very generic API and can also be used to schedule various periodic tasks in the cluster, such as data backups, log cleanup, etc.
System Admins can also perform ad-hoc tasks, such as on-demand backups or a system command (such as rm -rf ;) across all nodes of the cluster

```
ClusterMessagingService messagingService = manager.getMessagingService();

// Construct the Message
Message requestBackupUriRequest = new Message(
    MessageType.USER_DEFINE_MSG, UUID.randomUUID().toString());
requestBackupUriRequest
    .setMsgSubType(BootstrapProcess.REQUEST_BOOTSTRAP_URL);
requestBackupUriRequest.setMsgState(MessageState.NEW);

// Set the Recipient criteria: all nodes that satisfy the criteria will receive the message
Criteria recipientCriteria = new Criteria();
recipientCriteria.setInstanceName("%");
recipientCriteria.setRecipientInstanceType(InstanceType.PARTICIPANT);
recipientCriteria.setResource("MyDB");
recipientCriteria.setPartition("");

// Should be processed only by process(es) that are active at the time of sending the message
// This means if the recipient is restarted after message is sent, it will not be processe.
recipientCriteria.setSessionSpecific(true);

// wait for 30 seconds
int timeout = 30000;

// the handler that will be invoked when any recipient responds to the message.
BootstrapReplyHandler responseHandler = new BootstrapReplyHandler();

// this will return only after all recipients respond or after timeout
int sentMessageCount = messagingService.sendAndWait(recipientCriteria,
    requestBackupUriRequest, responseHandler, timeout);
```

See HelixManager.DefaultMessagingService in the [Javadocs](http://helix.apache.org/javadocs/0.7.0-incubating/reference/org/apache/helix/messaging/DefaultMessagingService.html) for more information.
