package org.apache.helix.integration.manager;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.ZkSystemPropertyKeys;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Runs a small cluster with the zk.zkclient.persistwatcher.enabled system property set, so that
 * every HelixManager registers persistent ZooKeeper watches instead of one-time watches, and checks
 * that a HelixManagerProperty client config can override the property per manager.
 */
public class TestZKHelixManagerPersistWatcher extends ZkUnitTestBase {
  private static final int NUM_NODES = 2;
  private static final int START_PORT = 12918;

  private final String _clusterName = TestHelper.getTestClassName();
  private final List<MockParticipantManager> _participants = new ArrayList<>();
  private ClusterControllerManager _controller;
  private String _originalPersistWatcherProperty;

  @BeforeClass
  public void beforeClass() throws Exception {
    _originalPersistWatcherProperty =
        System.getProperty(ZkSystemPropertyKeys.ZK_PERSIST_WATCHER_ENABLED);
    System.setProperty(ZkSystemPropertyKeys.ZK_PERSIST_WATCHER_ENABLED, "true");

    TestHelper.setupCluster(_clusterName, ZK_ADDR, START_PORT, "localhost", "TestDB", 1, 4,
        NUM_NODES, 2, "MasterSlave", true);
    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, "controller_0");
    _controller.syncStart();
    for (int i = 0; i < NUM_NODES; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, _clusterName, "localhost_" + (START_PORT + i));
      participant.syncStart();
      _participants.add(participant);
    }
  }

  @AfterClass
  public void afterClass() {
    try {
      for (MockParticipantManager participant : _participants) {
        if (participant.isConnected()) {
          participant.syncStop();
        }
      }
      if (_controller != null && _controller.isConnected()) {
        _controller.syncStop();
      }
      deleteCluster(_clusterName);
    } finally {
      if (_originalPersistWatcherProperty == null) {
        System.clearProperty(ZkSystemPropertyKeys.ZK_PERSIST_WATCHER_ENABLED);
      } else {
        System.setProperty(ZkSystemPropertyKeys.ZK_PERSIST_WATCHER_ENABLED,
            _originalPersistWatcherProperty);
      }
    }
  }

  @Test
  public void testClusterConvergesWithPersistentWatches() throws Exception {
    ZkHelixClusterVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    try {
      boolean converged = verifier.verifyByPolling();
      if (!converged) {
        dumpClusterState();
      }
      Assert.assertTrue(converged, "Cluster did not converge in persist watcher mode");
    } finally {
      verifier.close();
    }

    // Every manager created while the property is set registers persistent watches only.
    List<RealmAwareZkClient> clients = new ArrayList<>();
    clients.add(_controller.getZkClient());
    for (MockParticipantManager participant : _participants) {
      clients.add(participant.getZkClient());
    }
    for (RealmAwareZkClient client : clients) {
      Map<String, List<String>> watches = ZkTestHelper.getZkWatch(client);
      Assert.assertFalse(watches.get("persistentWatches").isEmpty(),
          "Expected persistent watches, got " + watches);
      Assert.assertTrue(watches.get("dataWatches").isEmpty(), "Unexpected one-time watches " + watches);
      Assert.assertTrue(watches.get("childWatches").isEmpty(), "Unexpected one-time watches " + watches);
      Assert.assertTrue(watches.get("existWatches").isEmpty(), "Unexpected one-time watches " + watches);
    }
  }

  @Test
  public void testHelixManagerPropertyOverridesSystemProperty() throws Exception {
    // The system property turns the mode on; an explicit client config turns it off for this manager.
    HelixManagerProperty property = new HelixManagerProperty.Builder().setRealmAwareZkClientConfig(
        new RealmAwareZkClient.RealmAwareZkClientConfig().setUsePersistWatcher(false)).build();
    SpectatorWithProperty spectator = new SpectatorWithProperty(_clusterName, "spectator_override",
        property);
    spectator.connect();
    try {
      String path = PropertyPathBuilder.idealState(_clusterName, "TestDB0");
      // A one-time watch is accepted, which a client in persist watcher mode refuses.
      Assert.assertNotNull(spectator.getZkClient().readData(path, new Stat(), true),
          "The ideal state should be readable");
      Map<String, List<String>> watches = ZkTestHelper.getZkWatch(spectator.getZkClient());
      Assert.assertTrue(watches.get("dataWatches").contains(path),
          "Expected a one-time watch on " + path + ", got " + watches);
      Assert.assertTrue(watches.get("persistentWatches").isEmpty(),
          "Unexpected persistent watches " + watches);
    } finally {
      spectator.disconnect();
    }
  }

  /** Prints what the controller and participants see so a non-converging run is diagnosable. */
  private void dumpClusterState() {
    System.out.println("=== Cluster state of " + _clusterName + " ===");
    for (String path : new String[] { PropertyPathBuilder.liveInstance(_clusterName),
        PropertyPathBuilder.idealState(_clusterName), PropertyPathBuilder.externalView(_clusterName),
        PropertyPathBuilder.controller(_clusterName) }) {
      dumpSubtree(path, 0);
    }
    for (String instance : _gZkClient.getChildren(PropertyPathBuilder.instance(_clusterName))) {
      dumpSubtree(PropertyPathBuilder.instanceCurrentState(_clusterName, instance), 0);
      dumpSubtree(PropertyPathBuilder.instanceMessage(_clusterName, instance), 0);
    }
  }

  private void dumpSubtree(String path, int depth) {
    if (!_gZkClient.exists(path)) {
      System.out.println(path + " (missing)");
      return;
    }
    Object data = _gZkClient.readData(path, true);
    System.out.println(path + (data == null ? "" : " -> " + data));
    if (depth < 3) {
      for (String child : _gZkClient.getChildren(path)) {
        dumpSubtree(path + "/" + child, depth + 1);
      }
    }
  }

  private static class SpectatorWithProperty extends ZKHelixManager {
    SpectatorWithProperty(String clusterName, String instanceName, HelixManagerProperty property) {
      super(clusterName, instanceName, InstanceType.SPECTATOR, ZK_ADDR, null, property);
    }

    RealmAwareZkClient getZkClient() {
      return _zkclient;
    }
  }
}
