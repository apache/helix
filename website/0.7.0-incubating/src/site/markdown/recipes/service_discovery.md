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
Service Discovery
-----------------

One of the common usage of ZooKeeper is to enable service discovery.
The basic idea is that when a server starts up it advertises its configuration/metadata such as its hostname and port on ZooKeeper.
This allows clients to dynamically discover the servers that are currently active. One can think of this like a service registry to which a server registers when it starts and
is automatically deregistered when it shutdowns or crashes. In many cases it serves as an alternative to VIPs.

The core idea behind this is to use ZooKeeper ephemeral nodes. The ephemeral nodes are created when the server registers and all its metadata is put into a ZNode.
When the server shutdowns, ZooKeeper automatically removes this ZNode.

There are two ways the clients can dynamically discover the active servers:

### ZooKeeper Watch

Clients can set a child watch under specific path on ZooKeeper.
When a new service is registered/deregistered, ZooKeeper notifies the client via a watch event and the client can read the list of services. Even though this looks trivial,
there are lot of things one needs to keep in mind like ensuring that you first set the watch back on ZooKeeper before reading data.


### Poll

Another approach is for the client to periodically read the ZooKeeper path and get the list of services.

Both approaches have pros and cons, for example setting a watch might trigger herd effect if there are large number of clients. This is problematic, especially when servers are starting up.
But the advantage to setting watches is that clients are immediately notified of a change which is not true in case of polling.
In some cases, having both watches and polls makes sense; watch allows one to get notifications as soon as possible while poll provides a safety net if a watch event is missed because of code bug or ZooKeeper fails to notify.

### Other Developer Considerations
* What happens when the ZooKeeper session expires? All the watches and ephemeral nodes previously added or created by this server are lost. One needs to add the watches again, recreate the ephemeral nodes, and so on.
* Due to network issues or Java GC pauses session expiry might happen again and again; this phenomenon is known as flapping. It\'s important for the server to detect this and deregister itself.

### Other Operational Considerations
* What if the node is behaving badly? One might kill the server, but it will lose the ability to debug. It would be nice to have the ability to mark a server as disabled and clients know that a node is disabled and will not contact that node.

### Configuration Ownership

This is an important aspect that is often ignored in the initial stages of your development. Typically, the service discovery pattern means that servers start up with some configuration which it simply puts into ZooKeeper. While this works well in the beginning, configuration management becomes very difficult since the servers themselves are statically configured. Any change in server configuration implies restarting the server. Ideally, it will be nice to have the ability to change configuration dynamically without having to restart a server.

Ideally you want a hybrid solution, a node starts with minimal configuration and gets the rest of configuration from ZooKeeper.

### Using Helix for Service Discovery

Even though Helix has a higher-level abstraction in terms of state machines, constraints and objectives, service discovery is one of things has been a prevalent use case from the start.
The controller uses the exact mechanism we described above to discover when new servers join the cluster. We create these ZNodes under /CLUSTERNAME/LIVEINSTANCES.
Since at any time there is only one controller, we use a ZK watch to track the liveness of a server.

This recipe simply demonstrates how one can re-use that part for implementing service discovery. This demonstrates multiple modes of service discovery:

* POLL: The client reads from zookeeper at regular intervals 30 seconds. Use this if you have 100's of clients
* WATCH: The client sets up watcher and gets notified of the changes. Use this if you have 10's of clients
* NONE: This does neither of the above, but reads directly from zookeeper when ever needed

Helix provides these additional features compared to other implementations available elsewhere:

* It has the concept of disabling a node which means that a badly behaving node can be disabled using the Helix admin API
* It automatically detects if a node connects/disconnects from zookeeper repeatedly and disables the node
* Configuration management
    * Allows one to set configuration via the admin API at various granulaties like cluster, instance, resource, partition
    * Configurations can be dynamically changed
    * The server is notified when configurations change


### Checkout and Build

```
git clone https://git-wip-us.apache.org/repos/asf/helix.git
cd helix
git checkout tags/helix-0.7.0-incubating
mvn clean install package -DskipTests
cd recipes/service-discovery/target/service-discovery-pkg/bin
chmod +x *
```

### Start ZooKeeper

```
./start-standalone-zookeeper 2199
```

### Run the Demo

```
./service-discovery-demo.sh
```

### Output

```
START:Service discovery demo mode:WATCH
	Registering service
		host.x.y.z_12000
		host.x.y.z_12001
		host.x.y.z_12002
		host.x.y.z_12003
		host.x.y.z_12004
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12000
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12002
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Deregistering service:
		host.x.y.z_12002
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12000
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Registering service:host.x.y.z_12002
END:Service discovery demo mode:WATCH
=============================================
START:Service discovery demo mode:POLL
	Registering service
		host.x.y.z_12000
		host.x.y.z_12001
		host.x.y.z_12002
		host.x.y.z_12003
		host.x.y.z_12004
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12000
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12002
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Deregistering service:
		host.x.y.z_12002
	Sleeping for poll interval:30000
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12000
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Registering service:host.x.y.z_12002
END:Service discovery demo mode:POLL
=============================================
START:Service discovery demo mode:NONE
	Registering service
		host.x.y.z_12000
		host.x.y.z_12001
		host.x.y.z_12002
		host.x.y.z_12003
		host.x.y.z_12004
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12000
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12002
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Deregistering service:
		host.x.y.z_12000
	SERVICES AVAILABLE
		SERVICENAME 	HOST 			PORT
		myServiceName 	host.x.y.z 		12001
		myServiceName 	host.x.y.z 		12002
		myServiceName 	host.x.y.z 		12003
		myServiceName 	host.x.y.z 		12004
	Registering service:host.x.y.z_12000
END:Service discovery demo mode:NONE
=============================================
```
