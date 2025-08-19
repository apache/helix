package org.apache.helix.manager.zk;

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

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.zookeeper.impl.client.DedicatedZkClient;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.IZkStateListener;
import org.apache.helix.zookeeper.zkclient.ZkEventThread;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for stale session event handling to prevent zombie participant conditions.
 */
public class TestStaleSessionEvents extends ZkTestBase {
  private static final String CLUSTER_PREFIX = "CLUSTER";
  private static final String MSDS_HOSTNAME = "localhost";
  private static final int MSDS_PORT = 19922;
  private static final String MSDS_NAMESPACE = "testStaleSessionEvents";

  private static MockMetadataStoreDirectoryServer _msdsServer;


  @Test
  public void testStaleSessionEventDoesNotCauseZombieParticipant() throws Exception {
    String instanceName = "localhost_12346";
    int participantPort = 12346;
    long sessionTimeout = 10000L;

    String clusterName = CLUSTER_PREFIX + "_" + getShortClassName() + "_staleSessionTest";
    String originalMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    String originalMsdsEndpoint = System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
    String originalZkSessionTimeout = System.getProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT);

    // start with clean slate
    System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
    System.clearProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT);

    try {
      setupMultiZkEnvironment(clusterName, participantPort, sessionTimeout);

      // reset before ZKHelixManager creation to ensure clean state
      RoutingDataManager.getInstance().reset(true);

      ZKHelixManager manager = new ZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, ZK_ADDR);
      ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();

      try {
        manager.connect(); // <- this will create the LiveInstance node
        String actualSessionId = manager.getSessionId();

        // active sessionID processing working as expected
        verifySessionBehaviorCore(accessor, keyBuilder, instanceName, actualSessionId, manager);

        // stale session behavior with two different fake sessions
        for (int i = 0; i < 2; i++) {
          String fakeSessionId = "0x" + Long.toHexString(Double.doubleToLongBits(Math.random() + i * 1000));
          Assert.assertFalse(actualSessionId.equals(fakeSessionId),
              String.format("Actual and fake session %d should be different", i + 1));

          CountDownLatch eventProcessed = new CountDownLatch(1);
          AtomicReference<Exception> caughtException = new AtomicReference<>();

          sendStaleSessionEvent(manager, fakeSessionId, eventProcessed, caughtException);
          eventProcessed.await(3 * sessionTimeout, TimeUnit.MILLISECONDS); // wait for event to be processed

          verifyStaleSessionEventBehavior(accessor, keyBuilder, instanceName, actualSessionId,
              caughtException, manager);
        }

      } finally {
        try {
          manager.disconnect();
        } catch (Exception e) {
          // Ignore cleanup errors
        }
      }
    } finally {
      cleanupMultiZkEnvironment(clusterName);

      System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
      System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
      System.clearProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT);

      // Set original values if they were not null
      if (originalMultiZkEnabled != null) {
        System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, originalMultiZkEnabled);
      }
      if (originalMsdsEndpoint != null) {
        System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, originalMsdsEndpoint);
      }
      if (originalZkSessionTimeout != null) {
        System.setProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT, originalZkSessionTimeout);
      }
    }
  }

  private void setupMultiZkEnvironment(String clusterName, int participantPort,
      long sessionTimeoutMs) throws Exception {
    Map<String, Collection<String>> routingData = new HashMap<>();
    routingData.put(ZK_ADDR, Collections.singletonList("/" + clusterName));

    _msdsServer = new MockMetadataStoreDirectoryServer(MSDS_HOSTNAME, MSDS_PORT, MSDS_NAMESPACE, routingData);
    _msdsServer.startServer();

    String msdsEndpoint = "http://" + MSDS_HOSTNAME + ":" + MSDS_PORT + "/admin/v2/namespaces/" + MSDS_NAMESPACE;
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, msdsEndpoint);

    TestHelper.setupCluster(clusterName, ZK_ADDR, participantPort,
        "localhost", "TestDB", 1, 2, 2, 1,
        "MasterSlave", true);

    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "true");
    System.setProperty(SystemPropertyKeys.ZK_SESSION_TIMEOUT, Long.toString(sessionTimeoutMs));
  }

  private void sendStaleSessionEvent(ZKHelixManager manager, String fakeSessionId,
      CountDownLatch eventProcessed, AtomicReference<Exception> caughtException) throws Exception {

    DedicatedZkClient dedicatedZkClient = (DedicatedZkClient) manager._zkclient;
    Field zkClientField = DedicatedZkClient.class.getDeclaredField("_rawZkClient");
    zkClientField.setAccessible(true);
    ZkClient zkClient = (ZkClient) zkClientField.get(dedicatedZkClient);

    Class<?> zkClientSuperClass = zkClient.getClass().getSuperclass();

    Field stateListenerField = zkClientSuperClass.getDeclaredField("_stateListener");
    stateListenerField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<IZkStateListener> stateListeners = (Set<IZkStateListener>) stateListenerField.get(zkClient);

    Field eventThreadField = zkClientSuperClass.getDeclaredField("_eventThread");
    eventThreadField.setAccessible(true);
    ZkEventThread eventThread = (ZkEventThread) eventThreadField.get(zkClient);

    ZkEventThread.ZkEvent event = new ZkEventThread.ZkEvent("Stale session event") {
      @Override
      public void run() throws Exception {
        try {
          for (final IZkStateListener listener : stateListeners) {
            listener.handleNewSession(fakeSessionId);
          }
        } catch (Exception e) {
          caughtException.set(e);
        } finally {
          eventProcessed.countDown();
        }
      }
    };
    eventThread.send(event);
  }

  private void verifyPreConditions(ZKHelixDataAccessor accessor, PropertyKey.Builder keyBuilder,
      String instanceName, String expectedSessionId, ZKHelixManager manager) throws Exception {
    // Verify LiveInstance exists and has correct session ID
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    Assert.assertNotNull(liveInstance, "LiveInstance should exist before testing session behavior");
    Assert.assertEquals(liveInstance.getEphemeralOwner(), expectedSessionId,
        "LiveInstance should have current active session ID");

    // Verify callBackHandlers exist and are healthy
    Field handlersField = ZKHelixManager.class.getDeclaredField("_handlers");
    handlersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<CallbackHandler> handlers = (List<CallbackHandler>) handlersField.get(manager);

    Assert.assertNotNull(handlers, "CallbackHandlers should exist");
    Assert.assertFalse(handlers.isEmpty(), "CallbackHandlers should not be empty");

    long readyCount = handlers.stream().filter(CallbackHandler::isReady).count();
    Assert.assertTrue(readyCount > 0, "At least one callback handler should be ready");
  }

  private void verifySessionBehaviorCore(ZKHelixDataAccessor accessor, PropertyKey.Builder keyBuilder,
      String instanceName, String expectedSessionId, ZKHelixManager manager) throws Exception {
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance(instanceName));
    Assert.assertNotNull(liveInstance, "LiveInstance should exist after session event");

    Assert.assertEquals(liveInstance.getEphemeralOwner(), expectedSessionId,
        "LiveInstance ephemeral owner should match expected session ID");

    // verify handlers remain healthy
    Field handlersField = ZKHelixManager.class.getDeclaredField("_handlers");
    handlersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    List<CallbackHandler> handlers = (List<CallbackHandler>) handlersField.get(manager);

    if (handlers != null && !handlers.isEmpty()) {
      long readyCount = handlers.stream().filter(CallbackHandler::isReady).count();
      long resetCount = handlers.size() - readyCount;
      Assert.assertTrue(readyCount > 0 && resetCount == 0,
          String.format("Expected all handlers to be ready. Ready: %d, Reset: %d", readyCount, resetCount));
    }
  }

  private void verifyStaleSessionEventBehavior(ZKHelixDataAccessor accessor, PropertyKey.Builder keyBuilder,
      String instanceName, String actualSessionId,
      AtomicReference<Exception> caughtException, ZKHelixManager manager) throws Exception {

    Assert.assertNull(caughtException.get(), "Stale session event should not throw exceptions");
    verifySessionBehaviorCore(accessor, keyBuilder, instanceName, actualSessionId, manager);
  }

  private void cleanupMultiZkEnvironment(String clusterName) {
    TestHelper.dropCluster(clusterName, _gZkClient);

    if (_msdsServer != null) {
      _msdsServer.stopServer();
      _msdsServer = null;
    }

    // Reset RoutingDataManager to ensure clean state for next test
    RoutingDataManager.getInstance().reset(true);
  }
}
