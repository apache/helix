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

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.mock.participant.MockTransition;
import org.apache.helix.model.Message;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestParticipantManager extends ZkTestBase {

  private static Logger LOG = Logger.getLogger(TestParticipantManager.class);

  @Test
  public void simpleIntegrationTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 1;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        4, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    HelixManager participant =
        new ZKHelixManager(clusterName, "localhost_12918", InstanceType.PARTICIPANT, _zkaddr);
    participant.getStateMachineEngine().registerStateModelFactory(StateModelDefId.MasterSlave,
        new MockMSModelFactory());
    participant.connect();

    HelixManager controller =
        new ZKHelixManager(clusterName, "controller_0", InstanceType.CONTROLLER, _zkaddr);
    controller.connect();

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // cleanup
    controller.disconnect();
    participant.disconnect();

    // verify all live-instances and leader nodes are gone
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance("localhost_12918")));
    Assert.assertNull(accessor.getProperty(keyBuilder.controllerLeader()));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void simpleSessionExpiryTest() throws Exception {
    // Logger.getRootLogger().setLevel(Level.WARN);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;
    int n = 1;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);
    String oldSessionId = participants[0].getSessionId();

    // expire zk-connection on localhost_12918
    ZkTestHelper.expireSession(participants[0].getZkClient());

    // wait until session expiry callback happens
    TimeUnit.MILLISECONDS.sleep(100);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);
    String newSessionId = participants[0].getSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

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
      PartitionId partition = message.getPartitionId();
      if (instance.equals("localhost_12918") && partition.equals("TestDB0_0")
          && _done.getAndSet(true) == false) {
        _startCountdown.countDown();
        // this await will be interrupted since we cancel the task during handleNewSession
        _endCountdown.await();
      }
    }
  }

  @Test
  public void testSessionExpiryInTransition() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    final String clusterName = className + "_" + methodName;
    int n = 1;
    CountDownLatch startCountdown = new CountDownLatch(1);
    CountDownLatch endCountdown = new CountDownLatch(1);

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    // start controller
    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].setTransition(new SessionExpiryTransition(startCountdown, endCountdown));
      participants[i].syncStart();
    }

    // wait transition happens to trigger session expiry
    startCountdown.await();
    String oldSessionId = participants[0].getSessionId();
    System.out.println("oldSessionId: " + oldSessionId);
    ZkTestHelper.expireSession(participants[0].getZkClient());

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    String newSessionId = participants[0].getSessionId();
    Assert.assertNotSame(newSessionId, oldSessionId);

    // assert interrupt exception error in old session
    String errPath =
        PropertyPathConfig.getPath(PropertyType.ERRORS, clusterName, "localhost_12918",
            oldSessionId, "TestDB0", "TestDB0_0");
    ZNRecord error = _zkclient.readData(errPath);
    Assert
        .assertNotNull(
            error,
            "InterruptedException should happen in old session since task is being cancelled during handleNewSession");
    String errString = new String(new ZNRecordSerializer().serialize(error));
    Assert.assertTrue(errString.indexOf("InterruptedException") != -1);

    // cleanup
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
