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
  <title>Tutorial - Helix Agent</title>
</head>

## [Helix Tutorial](./Tutorial.html): Helix Agent (for non-JVM systems)

Not every distributed system is written on the JVM, but many systems would benefit from the cluster management features that Helix provides. To make a non-JVM system work with Helix, you can use the Helix Agent module.

### What is Helix Agent?

Helix is built on the following assumption: if your distributed resource is modeled by a finite state machine, then Helix can tell participants when they should transition between states. In the Java API, this means implementing transition callbacks. In the Helix agent API, this means providing commands than can run for each transition.

These commands could do anything behind the scenes; Helix only requires that they exit once the state transition is complete.

### Configuring Transition Commands

Here's how to tell Helix which commands to run on state transitions:

#### Java

Using the Java API, first get a configuration scope (the Helix agent supports both cluster and resource scopes, picking resource first if it is available):

```
// Cluster scope
HelixConfigScope scope =
    new HelixConfigScopeBuilder(ConfigScopeProperty.CLUSTER).forCluster(clusterName).build();

// Resource scope
HelixConfigScope scope =
    new HelixConfigScopeBuilder(ConfigScopeProperty.RESOURCE).forCluster(clusterName).forResource(resourceName).build();
```

Then, specify the command to run for each state transition:

```
// Get the configuration accessor
ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);

// Specify the script for OFFLINE --> ONLINE
CommandConfig.Builder builder = new CommandConfig.Builder();
CommandConfig cmdConfig =
    builder.setTransition("OFFLINE", "ONLINE").setCommand("simpleHttpClient.py OFFLINE-ONLINE")
        .setCommandWorkingDir(workingDir)
        .setCommandTimeout("5000L") // optional: ms to wait before failing
        .setPidFile(pidFile) // optional: for daemon-like systems that will write the process id to a file
        .build();
configAccessor.set(scope, cmdConfig.toKeyValueMap());

// Specify the script for ONLINE --> OFFLINE
builder = new CommandConfig.Builder();
cmdConfig =
    builder.setTransition("ONLINE", "OFFLINE").setCommand("simpleHttpClient.py ONLINE-OFFLINE")
        .setCommandWorkingDir(workingDir)
        .build();
configAccessor.set(scope, cmdConfig.toKeyValueMap());

// Specify NOP for OFFLINE --> DROPPED
builder = new CommandConfig.Builder();
cmdConfig =
    builder.setTransition("OFFLINE", "DROPPED")
        .setCommand(CommandAttribute.NOP.getName())
        .build();
configAccessor.set(scope, cmdConfig.toKeyValueMap());
```

In this example, we have a program called simpleHttpClient.py that we call for all transitions, only changing the arguments that are passed in. However, there is no requirement that each transition invoke the same program; this API allows running arbitrary commands in arbitrary directories with arbitrary arguments.

Notice that that for the OFFLINE \-\-\> DROPPED transition, we do not run any command (specifically, we specify the NOP command). This just tells Helix that the system doesn't care about when things are dropped, and it can consider the transition already done.

#### Command Line

It is also possible to configure everything directly from the command line. Here's how that would look for cluster-wide configuration:

```
# Specify the script for OFFLINE --> ONLINE
/helix-admin.sh --zkSvr localhost:2181 --setConfig CLUSTER clusterName OFFLINE-ONLINE.command="simpleHttpClient.py OFFLINE-ONLINE",OFFLINE-ONLINE.workingDir="/path/to/script", OFFLINE-ONLINE.command.pidfile="/path/to/pidfile"

# Specify the script for ONLINE --> OFFLINE
/helix-admin.sh --zkSvr localhost:2181 --setConfig CLUSTER clusterName ONLINE-OFFLINE.command="simpleHttpClient.py ONLINE-OFFLINE",ONLINE-OFFLINE.workingDir="/path/to/script", OFFLINE-ONLINE.command.pidfile="/path/to/pidfile"

# Specify NOP for OFFLINE --> DROPPED
/helix-admin.sh --zkSvr localhost:2181 --setConfig CLUSTER clusterName ONLINE-OFFLINE.command="nop"
```

Like in the Java configuration, it is also possible to specify a resource scope instead of a cluster scope:

```
# Specify the script for OFFLINE --> ONLINE
/helix-admin.sh --zkSvr localhost:2181 --setConfig RESOURCE clusterName,resourceName OFFLINE-ONLINE.command="simpleHttpClient.py OFFLINE-ONLINE",OFFLINE-ONLINE.workingDir="/path/to/script", OFFLINE-ONLINE.command.pidfile="/path/to/pidfile"
```

### Starting the Agent

There should be an agent running for every participant you have running. Ideally, its lifecycle should match that of the participant. Here, we have a simple long-running participant called simpleHttpServer.py. Its only purpose is to record state transitions.

Here are some ways that you can start the Helix agent:

#### Java

```
// Start your application process
ExternalCommand serverCmd = ExternalCommand.start(workingDir + "/simpleHttpServer.py");

// Start the agent
Thread agentThread = new Thread() {
  @Override
  public void run() {
    while(!isInterrupted()) {
      try {
        HelixAgentMain.main(new String[] {
            "--zkSvr", zkAddr, "--cluster", clusterName, "--instanceName", instanceName,
            "--stateModel", "OnlineOffline"
        });
      } catch (InterruptedException e) {
        LOG.info("Agent thread interrupted", e);
        interrupt();
      } catch (Exception e) {
        LOG.error("Exception start helix-agent", e);
      }
    }
  }
};
agentThread.start();

// Wait for the process to terminate (either intentionally or unintentionally)
serverCmd.waitFor();

// Kill the agent
agentThread.interrupt();
```

#### Command Line

```
# Build Helix and start the agent
mvn clean install -DskipTests
chmod +x helix-agent/target/helix-agent-pkg/bin/*
helix-agent/target/helix-agent-pkg/bin/start-helix-agent.sh --zkSvr zkAddr1,zkAddr2 --cluster clusterName --instanceName instanceName --stateModel OnlineOffline

# Here, you can define your own logic to terminate this agent when your process terminates
...
```

### Example

[Here](https://git-wip-us.apache.org/repos/asf?p=helix.git;a=blob;f=helix-agent/src/test/java/org/apache/helix/agent/TestHelixAgent.java;h=ccf64ce5544207c7e48261682ea69945b71da7f1;hb=refs/heads/master) is a basic system that uses the Helix agent package.

### Notes

As you may have noticed from the examples, the participant program and the state transition program are two different programs. The former is a _long-running_ process that is directly tied to the Helix agent. The latter is a process that only exists while a state transition is underway. Despite this, these two processes should be intertwined. The transition command will need to communicate to the participant to actually complete the state transition and the participant will need to communicate whether or not this was successful. The implementation of this protocol is the responsibility of the system.