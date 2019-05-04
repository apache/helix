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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.mbeans.HelixCallbackMonitor;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.MonitorLevel;
import org.apache.helix.monitoring.mbeans.ZkClientMonitor;
import org.apache.helix.monitoring.mbeans.ZkClientPathMonitor;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestParticipantManager extends ZkTestBase {
  private MBeanServer _server = ManagementFactory.getPlatformMBeanServer();
  private String clusterName = TestHelper.getTestClassName();

  @AfterMethod
  public void afterMethod(Method testMethod, ITestContext testContext) {
    deleteCluster(clusterName);
    super.endTest(testMethod, testContext);
  }

  @Test
  public void simpleIntegrationTest() throws Exception {
    int n = 1;

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    HelixManager participant =
        new ZKHelixManager(clusterName, "localhost_12918", InstanceType.PARTICIPANT, ZK_ADDR);
    participant.getStateMachineEngine().registerStateModelFactory("MasterSlave",
        new MockMSModelFactory());
    participant.connect();

    HelixManager controller =
        new ZKHelixManager(clusterName, "controller_0", InstanceType.CONTROLLER, ZK_ADDR);
    controller.connect();

    verifyHelixManagerMetrics(InstanceType.PARTICIPANT, MonitorLevel.DEFAULT,
        participant.getInstanceName());
    verifyHelixManagerMetrics(InstanceType.CONTROLLER, MonitorLevel.DEFAULT,
        controller.getInstanceName());

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    // cleanup
    controller.disconnect();
    participant.disconnect();

    // verify all live-instances and leader nodes are gone
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12918")));
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));
  }

  @Test // (dependsOnMethods = "simpleIntegrationTest")
  public void testMonitoringLevel() throws Exception {
    int n = 1;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    System.setProperty(SystemPropertyKeys.MONITOR_LEVEL, MonitorLevel.ALL.name());
    HelixManager participant;
    try {
      participant =
          new ZKHelixManager(clusterName, "localhost_12918", InstanceType.PARTICIPANT, ZK_ADDR);
    } finally {
      System.clearProperty(SystemPropertyKeys.MONITOR_LEVEL);
    }

    participant.getStateMachineEngine().registerStateModelFactory("MasterSlave",
        new MockMSModelFactory());
    participant.connect();

    verifyHelixManagerMetrics(InstanceType.PARTICIPANT, MonitorLevel.ALL,
        participant.getInstanceName());

    // cleanup
    participant.disconnect();
  }

  private void verifyHelixManagerMetrics(InstanceType type, MonitorLevel monitorLevel,
      String instanceName) throws MalformedObjectNameException {
    // check HelixCallback Monitor
    Set<ObjectInstance> objs =
        _server.queryMBeans(buildCallbackMonitorObjectName(type, clusterName, instanceName), null);
    Assert.assertEquals(objs.size(), 13);

    // check HelixZkClient Monitors
    objs =
        _server.queryMBeans(buildZkClientMonitorObjectName(type, clusterName, instanceName), null);
    Assert.assertEquals(objs.size(), 1);

    objs = _server.queryMBeans(buildZkClientPathMonitorObjectName(type, clusterName, instanceName),
        null);

    int expectedZkPathMonitor;
    switch (monitorLevel) {
    case ALL:
      expectedZkPathMonitor = 10;
      break;
    case AGGREGATED_ONLY:
      expectedZkPathMonitor = 1;
      break;
    default:
      expectedZkPathMonitor =
          type == InstanceType.CONTROLLER || type == InstanceType.CONTROLLER_PARTICIPANT ? 10 : 1;
    }
    Assert.assertEquals(objs.size(), expectedZkPathMonitor);
  }

  private ObjectName buildCallbackMonitorObjectName(InstanceType type, String cluster,
      String instance) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixCallback.name(),
        HelixCallbackMonitor.MONITOR_TYPE, type.name(), HelixCallbackMonitor.MONITOR_KEY,
        cluster + "." + instance, HelixCallbackMonitor.MONITOR_CHANGE_TYPE, "*");
  }

  private ObjectName buildZkClientMonitorObjectName(InstanceType type, String cluster,
      String instance) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixZkClient.name(),
        ZkClientMonitor.MONITOR_TYPE, type.name(), ZkClientMonitor.MONITOR_KEY,
        cluster + "." + instance);
  }

  private ObjectName buildZkClientPathMonitorObjectName(InstanceType type, String cluster,
      String instance) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.HelixZkClient.name(),
        ZkClientMonitor.MONITOR_TYPE, type.name(), ZkClientMonitor.MONITOR_KEY,
        cluster + "." + instance, ZkClientPathMonitor.MONITOR_PATH, "*");
  }

  @Test
  public void simpleSessionExpiryTest() throws Exception {
    int n = 1;

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());
    String oldSessionId = participants[0].getSessionId();

    // expire zk-connection on localhost_12918
    ZkTestHelper.expireSession(participants[0].getZkClient());

    // wait until session expiry callback happens
    TimeUnit.MILLISECONDS.sleep(100);

    Assert.assertTrue(verifier.verifyByPolling());
    String newSessionId = participants[0].getSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
  }

  class SessionExpiryTransition extends MockTransition {
    private final AtomicBoolean _done = new AtomicBoolean();
    private final CountDownLatch _startCountdown;
    private final CountDownLatch _endCountdown;

    public SessionExpiryTransition(CountDownLatch startCountdown, CountDownLatch endCountdown) {
      _startCountdown = startCountdown;
      _endCountdown = endCountdown;
    }

    @Override
    public void doTransition(Message message, NotificationContext context)
        throws InterruptedException {
      String instance = message.getTgtName();
      String partition = message.getPartitionName();
      if (instance.equals("localhost_12918") && partition.equals("TestDB0_0")
          && !_done.getAndSet(true)) {
        _startCountdown.countDown();
        // this await will be interrupted since we cancel the task during handleNewSession
        _endCountdown.await();
      }
    }
  }

  @Test
  public void testSessionExpiryInTransition() throws Exception {
    int n = 1;
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    MockParticipantManager[] participants = new MockParticipantManager[n];

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].setTransition(new SessionExpiryTransition(startCountdown, endCountdown));
      participants[i].syncStart();
    }

    // wait transition happens to trigger session expiry
    startCountdown.await();
    String oldSessionId = participants[0].getSessionId();
    ZkTestHelper.expireSession(participants[0].getZkClient());

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkClient(_gZkClient)
            .setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(verifier.verifyByPolling());

    String newSessionId = participants[0].getSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    // assert interrupt exception error in old session
    String errPath = PropertyPathBuilder.instanceError(clusterName, "localhost_12918", oldSessionId,
        "TestDB0", "TestDB0_0");
    ZNRecord error = _gZkClient.readData(errPath);
    Assert.assertNotNull(error,
        "InterruptedException should happen in old session since task is being cancelled during handleNewSession");
    String errString = new String(new ZNRecordSerializer().serialize(error));
    Assert.assertTrue(errString.contains("InterruptedException"));

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
  }
}
