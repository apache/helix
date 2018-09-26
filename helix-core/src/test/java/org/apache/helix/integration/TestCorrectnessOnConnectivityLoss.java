package org.apache.helix.integration;

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

import com.google.common.collect.Maps;

import java.lang.reflect.Method;
import java.util.Map;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestCorrectnessOnConnectivityLoss {
  private static final String ZK_ADDR = "localhost:21892";
  private ZkServer _zkServer;
  private String _clusterName;
  private ClusterControllerManager _controller;

  @BeforeMethod
  public void beforeMethod(Method testMethod) throws Exception {
    _zkServer = TestHelper.startZkServer(ZK_ADDR, null, false);

    String className = TestHelper.getTestClassName();
    String methodName = testMethod.getName();
    _clusterName = className + "_" + methodName;
    TestHelper.setupCluster(_clusterName, ZK_ADDR, 12918, // participant start port
        "localhost", // participant host
        "resource", // resource name prefix
        1, // number of resources
        1, // number of partitions
        1, // number of participants
        1, // number of replicas
        "OnlineOffline", // state model
        RebalanceMode.FULL_AUTO, // automatic assignment
        true); // rebalance

    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller0");
    _controller.connect();
  }

  @AfterMethod
  public void afterMethod() {
    TestHelper.stopZkServer(_zkServer);
  }

  @Test
  public void testParticipant() throws Exception {
    Map<String, Integer> stateReachedCounts = Maps.newHashMap();
    HelixManager participant =
        HelixManagerFactory.getZKHelixManager(_clusterName, "localhost_12918",
            InstanceType.PARTICIPANT, ZK_ADDR);
    participant.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new MyStateModelFactory(stateReachedCounts));
    participant.connect();

    Thread.sleep(1000);

    // Ensure that the external view coalesces
    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            _clusterName));
    Assert.assertTrue(result);

    // Ensure that there was only one state transition
    Assert.assertEquals(stateReachedCounts.size(), 1);
    Assert.assertTrue(stateReachedCounts.containsKey("ONLINE"));
    Assert.assertEquals(stateReachedCounts.get("ONLINE").intValue(), 1);

    // Now let's stop the ZK server; this should do nothing
    TestHelper.stopZkServer(_zkServer);
    Thread.sleep(1000);

    // Verify no change
    Assert.assertEquals(stateReachedCounts.size(), 1);
    Assert.assertTrue(stateReachedCounts.containsKey("ONLINE"));
    Assert.assertEquals(stateReachedCounts.get("ONLINE").intValue(), 1);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSpectator() throws Exception {
    Map<String, Integer> stateReachedCounts = Maps.newHashMap();
    HelixManager participant =
        HelixManagerFactory.getZKHelixManager(_clusterName, "localhost_12918",
            InstanceType.PARTICIPANT, ZK_ADDR);
    participant.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new MyStateModelFactory(stateReachedCounts));
    participant.connect();

    RoutingTableProvider routingTableProvider = new RoutingTableProvider();
    try {
      HelixManager spectator = HelixManagerFactory
          .getZKHelixManager(_clusterName, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
      spectator.connect();
      spectator.addConfigChangeListener(routingTableProvider);
      spectator.addExternalViewChangeListener(routingTableProvider);
      Thread.sleep(1000);

      // Now let's stop the ZK server; this should do nothing
      TestHelper.stopZkServer(_zkServer);
      Thread.sleep(1000);

      // Verify routing table still works
      Assert.assertEquals(routingTableProvider.getInstances("resource0", "ONLINE").size(), 1);
      Assert.assertEquals(routingTableProvider.getInstances("resource0", "OFFLINE").size(), 0);
    } finally {
      routingTableProvider.shutdown();
    }
  }

  @StateModelInfo(initialState = "OFFLINE", states = {
      "MASTER", "SLAVE", "OFFLINE", "ERROR"
  })
  public static class MyStateModel extends StateModel {
    private final Map<String, Integer> _counts;

    public MyStateModel(Map<String, Integer> counts) {
      _counts = counts;
    }

    @Transition(to = "ONLINE", from = "OFFLINE")
    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      incrementCount(message.getToState());
    }

    @Transition(to = "OFFLINE", from = "ONLINE")
    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      incrementCount(message.getToState());
    }

    @Transition(to = "DROPPED", from = "OFFLINE")
    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      incrementCount(message.getToState());
    }

    @Transition(to = "OFFLINE", from = "ERROR")
    public void onBecomeOfflineFromError(Message message, NotificationContext context) {
      incrementCount(message.getToState());
    }

    @Transition(to = "DROPPED", from = "ERROR")
    public void onBecomeDroppedFromError(Message message, NotificationContext context) {
      incrementCount(message.getToState());
    }

    @Override
    public void rollbackOnError(Message message, NotificationContext context,
        StateTransitionError error) {
      incrementCount("rollback");
    }

    private synchronized void incrementCount(String toState) {
      int current = (_counts.containsKey(toState)) ? _counts.get(toState) : 0;
      _counts.put(toState, current + 1);
    }
  }

  public static class MyStateModelFactory extends StateModelFactory<MyStateModel> {

    private final Map<String, Integer> _counts;

    public MyStateModelFactory(Map<String, Integer> counts) {
      _counts = counts;
    }

    @Override
    public MyStateModel createNewStateModel(String resource, String partitionId) {
      return new MyStateModel(_counts);
    }
  }
}
