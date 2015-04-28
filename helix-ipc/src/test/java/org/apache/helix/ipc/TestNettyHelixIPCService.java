package org.apache.helix.ipc;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.ipc.netty.NettyHelixIPCService;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.resolver.HelixAddress;
import org.apache.helix.resolver.HelixMessageScope;
import org.apache.helix.resolver.HelixResolver;
import org.apache.helix.resolver.zk.ZKHelixResolver;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestNettyHelixIPCService extends ZkTestBase {

  private static final Logger LOG = Logger.getLogger(TestNettyHelixIPCService.class);

  private static final String CLUSTER_NAME = "TEST_CLUSTER";
  private static final String RESOURCE_NAME = "MyResource";

  private int firstPort;
  private int secondPort;
  private HelixManager controller;
  private HelixManager firstNode;
  private HelixManager secondNode;
  private HelixResolver firstResolver;
  private HelixResolver secondResolver;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Allocate test resources
    firstPort = TestHelper.getRandomPort();
    secondPort = TestHelper.getRandomPort();

    // Setup cluster
    ClusterSetup clusterSetup = new ClusterSetup(_zkaddr);
    clusterSetup.addCluster(CLUSTER_NAME, true);
    clusterSetup.addInstanceToCluster(CLUSTER_NAME, "localhost_" + firstPort);
    clusterSetup.addInstanceToCluster(CLUSTER_NAME, "localhost_" + secondPort);

    // Start Helix agents
    controller =
        HelixControllerMain.startHelixController(_zkaddr, CLUSTER_NAME, "CONTROLLER", "STANDALONE");
    firstNode =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + firstPort,
            InstanceType.PARTICIPANT, _zkaddr);
    secondNode =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "localhost_" + secondPort,
            InstanceType.PARTICIPANT, _zkaddr);

    // Connect participants
    firstNode.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("OnlineOffline"), new DummyStateModelFactory());
    firstNode.connect();
    secondNode.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.from("OnlineOffline"), new DummyStateModelFactory());
    secondNode.connect();

    // Add a resource
    clusterSetup.addResourceToCluster(CLUSTER_NAME, RESOURCE_NAME, 4, "OnlineOffline");
    clusterSetup.rebalanceResource(CLUSTER_NAME, RESOURCE_NAME, 1);

    // Wait for External view convergence
    ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
        _zkaddr, CLUSTER_NAME), 10000);

    // Connect resolvers
    firstResolver = new ZKHelixResolver(_zkaddr);
    firstResolver.connect();
    secondResolver = new ZKHelixResolver(_zkaddr);
    secondResolver.connect();

    // Configure
    firstNode.getConfigAccessor().set(
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
            .forCluster(firstNode.getClusterName()).forParticipant(firstNode.getInstanceName())
            .build(), HelixIPCService.IPC_PORT, String.valueOf(firstPort));
    secondNode.getConfigAccessor().set(
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.PARTICIPANT)
            .forCluster(secondNode.getClusterName()).forParticipant(secondNode.getInstanceName())
            .build(), HelixIPCService.IPC_PORT, String.valueOf(secondPort));
  }

  @AfterClass
  public void afterClass() throws Exception {
    firstNode.disconnect();
    secondNode.disconnect();
    controller.disconnect();
  }

  @Test
  public void testService() throws Exception {
    final int numMessages = 1000;
    final int messageType = 1;

    // Start first IPC service w/ counter
    final ConcurrentMap<String, AtomicInteger> firstCounts =
        new ConcurrentHashMap<String, AtomicInteger>();
    final HelixIPCService firstIPC =
        new NettyHelixIPCService(new NettyHelixIPCService.Config().setInstanceName(
            firstNode.getInstanceName()).setPort(firstPort));
    firstIPC.registerCallback(messageType, new HelixIPCCallback() {
      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        String key = scope.getPartition() + ":" + scope.getState();
        firstCounts.putIfAbsent(key, new AtomicInteger());
        firstCounts.get(key).incrementAndGet();
      }
    });
    firstIPC.start();

    // Start second IPC Service w/ counter
    final ConcurrentMap<String, AtomicInteger> secondCounts =
        new ConcurrentHashMap<String, AtomicInteger>();
    final HelixIPCService secondIPC =
        new NettyHelixIPCService(new NettyHelixIPCService.Config().setInstanceName(
            secondNode.getInstanceName()).setPort(secondPort));
    secondIPC.registerCallback(messageType, new HelixIPCCallback() {
      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        String key = scope.getPartition() + ":" + scope.getState();
        secondCounts.putIfAbsent(key, new AtomicInteger());
        secondCounts.get(key).incrementAndGet();
      }
    });
    secondIPC.start();

    // Allow resolver callbacks to fire
    Thread.sleep(500);

    // Find all partitions on second node...
    String secondName = "localhost_" + secondPort;
    Set<String> secondPartitions = new HashSet<String>();
    IdealState idealState =
        controller.getClusterManagmentTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    for (String partitionName : idealState.getPartitionSet()) {
      for (Map.Entry<String, String> stateEntry : idealState.getInstanceStateMap(partitionName)
          .entrySet()) {
        if (stateEntry.getKey().equals(secondName)) {
          secondPartitions.add(partitionName);
        }
      }
    }

    // And use first node to send messages to them
    for (String partitionName : secondPartitions) {
      for (int i = 0; i < numMessages; i++) {
        HelixMessageScope scope =
            new HelixMessageScope.Builder().cluster(firstNode.getClusterName())
                .resource(RESOURCE_NAME).partition(partitionName).state("ONLINE").build();

        Set<HelixAddress> destinations = firstResolver.getDestinations(scope);
        for (HelixAddress destination : destinations) {
          ByteBuf message = Unpooled.wrappedBuffer(("Hello" + i).getBytes());
          firstIPC.send(destination, messageType, UUID.randomUUID(), message);
        }
      }
    }

    // Loopback
    for (String partitionName : secondPartitions) {
      for (int i = 0; i < numMessages; i++) {
        HelixMessageScope scope =
            new HelixMessageScope.Builder().cluster(secondNode.getClusterName())
                .resource(RESOURCE_NAME).partition(partitionName).state("ONLINE").build();

        Set<HelixAddress> destinations = secondResolver.getDestinations(scope);
        for (HelixAddress destination : destinations) {
          ByteBuf message = Unpooled.wrappedBuffer(("Hello" + i).getBytes());
          secondIPC.send(destination, messageType, UUID.randomUUID(), message);
        }
      }
    }

    // Check
    Thread.sleep(500); // just in case
    for (String partitionName : secondPartitions) {
      AtomicInteger count = secondCounts.get(partitionName + ":ONLINE");
      Assert.assertNotNull(count);
      Assert.assertEquals(count.get(), 2 * numMessages);
    }

    // Shutdown
    firstIPC.shutdown();
    secondIPC.shutdown();
  }

  @Test
  public void testMessageManager() throws Exception {
    final int numMessages = 1000;
    final int messageType = 1;
    final int ackMessageType = 2;

    // First IPC service
    final HelixIPCService firstIPC =
        new NettyHelixIPCService(new NettyHelixIPCService.Config().setInstanceName(
            firstNode.getInstanceName()).setPort(firstPort));
    firstIPC.registerCallback(messageType, new HelixIPCCallback() {
      final Random random = new Random();

      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        if (random.nextInt() % 2 == 0) {
          HelixAddress sender = firstResolver.getSource(scope);
          firstIPC.send(sender, ackMessageType, messageId, null);
        }
      }
    });
    firstIPC.start();

    // Second IPC service
    final HelixIPCService secondIPC =
        new NettyHelixIPCService(new NettyHelixIPCService.Config().setInstanceName(
            secondNode.getInstanceName()).setPort(secondPort));
    secondIPC.registerCallback(messageType, new HelixIPCCallback() {
      final Random random = new Random();

      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        if (random.nextInt() % 2 == 0) {
          HelixAddress sender = secondResolver.getSource(scope);
          secondIPC.send(sender, ackMessageType, messageId, null);
        }
      }
    });
    secondIPC.start();

    // Allow resolver callbacks to fire
    Thread.sleep(500);

    // Start state machine (uses first, sends to second)
    final AtomicInteger numAcks = new AtomicInteger();
    final AtomicInteger numErrors = new AtomicInteger();
    HelixIPCService messageManager =
        new HelixIPCMessageManager(Executors.newSingleThreadScheduledExecutor(), firstIPC, 300, -1);
    messageManager.registerCallback(ackMessageType, new HelixIPCCallback() {
      @Override
      public void onMessage(HelixMessageScope scope, UUID messageId, ByteBuf message) {
        numAcks.incrementAndGet();
      }
    });
    messageManager.start();

    // Find all partitions on second node...
    String secondName = "localhost_" + secondPort;
    Set<String> secondPartitions = new HashSet<String>();
    IdealState idealState =
        controller.getClusterManagmentTool().getResourceIdealState(CLUSTER_NAME, RESOURCE_NAME);
    for (String partitionName : idealState.getPartitionSet()) {
      for (Map.Entry<String, String> stateEntry : idealState.getInstanceStateMap(partitionName)
          .entrySet()) {
        if (stateEntry.getKey().equals(secondName)) {
          secondPartitions.add(partitionName);
        }
      }
    }

    // And use first node to send messages to them
    for (String partitionName : secondPartitions) {
      for (int i = 0; i < numMessages; i++) {
        HelixMessageScope scope =
            new HelixMessageScope.Builder().cluster(firstNode.getClusterName())
                .resource(RESOURCE_NAME).partition(partitionName).state("ONLINE").build();
        Set<HelixAddress> destinations = firstResolver.getDestinations(scope);
        for (HelixAddress destination : destinations) {
          ByteBuf message = Unpooled.wrappedBuffer(("Hello" + i).getBytes());
          messageManager.send(destination, messageType, UUID.randomUUID(), message);
        }
      }
    }

    // Ensure they're all ack'ed (tests retry logic because only every other one is acked)
    Thread.sleep(5000);
    Assert.assertEquals(numAcks.get() + numErrors.get(), numMessages * secondPartitions.size());

    // Shutdown
    messageManager.shutdown();
    firstIPC.shutdown();
    secondIPC.shutdown();
  }

  public static class DummyStateModelFactory extends
      StateTransitionHandlerFactory<TransitionHandler> {
    @Override
    public TransitionHandler createStateTransitionHandler(ResourceId resourceId, PartitionId partitionId) {
      return new DummyStateModel();
    }

    @StateModelInfo(states = "{'OFFLINE', 'ONLINE'}", initialState = "OFFLINE")
    public static class DummyStateModel extends TransitionHandler {
      @Transition(from = "OFFLINE", to = "ONLINE")
      public void fromOfflineToOnline(Message message, NotificationContext context) {
        LOG.info(message);
      }

      @Transition(from = "ONLINE", to = "OFFLINE")
      public void fromOnlineToOffline(Message message, NotificationContext context) {
        LOG.info(message);
      }
    }
  }
}
