package org.apache.helix.integration.messaging;

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

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixProperty.HelixPropertyAttribute;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.participant.ErrTransition;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBatchMessage extends ZkTestBase {
  class TestZkChildListener implements IZkChildListener {
    int _maxNumberOfChildren = 0;

    @Override
    public void handleChildChange(String parentPath, List<String> currentChildren) {
      if (currentChildren == null) {
        return;
      }
      System.out.println(parentPath + " has " + currentChildren.size() + " messages");
      if (currentChildren.size() > _maxNumberOfChildren) {
        _maxNumberOfChildren = currentChildren.size();
      }
    }

  }

  @Test
  public void testBasic() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // enable batch message
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // register a message listener so we know how many message generated
    TestZkChildListener listener = new TestZkChildListener();
    _gZkClient.subscribeChildChanges(keyBuilder.messages("localhost_12918").getPath(), listener);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    BestPossAndExtViewZkVerifier verifier = new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName);
    try {
      boolean result = ClusterStateVerifier
          .verifyByZkCallback(verifier);
      Assert.assertTrue(result);
      // Change to three is because there is an extra factory registered
      // So one extra NO_OP message send
      Assert.assertTrue(listener._maxNumberOfChildren <= 3,
          "Should get no more than 2 messages (O->S and S->M)");
    } finally {
      verifier.close();
    }

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // a non-batch-message run followed by a batch-message-enabled run
  @Test
  public void testChangeBatchMessageMode() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    BestPossAndExtViewZkVerifier verifier = new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName);
    try {
      boolean result = ClusterStateVerifier.verifyByZkCallback(verifier);
      Assert.assertTrue(result);
    } finally {
      verifier.close();
    }

    // stop all participants
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    // enable batch message
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // registry a message listener so we know how many message generated
    TestZkChildListener listener = new TestZkChildListener();
    _gZkClient.subscribeChildChanges(keyBuilder.messages("localhost_12918").getPath(), listener);

    // restart all participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    verifier = new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName);
    try {
      boolean result = ClusterStateVerifier
          .verifyByZkCallback(verifier);
      Assert.assertTrue(result);
      // Change to three is because there is an extra factory registered
      // So one extra NO_OP message send
      Assert.assertTrue(listener._maxNumberOfChildren <= 3,
          "Should get no more than 2 messages (O->S and S->M)");
    } finally {
      verifier.close();
    }

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSubMsgExecutionFail() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    final int n = 5;
    MockParticipantManager[] participants = new MockParticipantManager[n];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, "localhost", "TestDB", 1, // resource#
        6, // partition#
        n, // nodes#
        3, // replicas#
        "MasterSlave", true);

    // enable batch message
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    // get MASTER for errPartition
    String errPartition = "TestDB0_0";
    String masterOfPartition0 = null;
    for (Map.Entry<String, String> entry : idealState.getInstanceStateMap(errPartition).entrySet()) {
      if (entry.getValue().equals("MASTER")) {
        masterOfPartition0 = entry.getKey();
        break;
      }
    }
    Assert.assertNotNull(masterOfPartition0);

    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName);
    controller.syncStart();

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      if (instanceName.equals(masterOfPartition0)) {
        Map<String, Set<String>> errPartitions = new HashMap<String, Set<String>>();
        errPartitions.put("SLAVE-MASTER", TestHelper.setOf("TestDB0_0"));
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
        participants[i].setTransition(new ErrTransition(errPartitions));
      } else {
        participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      }
      participants[i].syncStart();
    }

    Map<String, Map<String, String>> errStates = new HashMap<String, Map<String, String>>();
    errStates.put("TestDB0", new HashMap<String, String>());
    errStates.get("TestDB0").put(errPartition, masterOfPartition0);

    BestPossAndExtViewZkVerifier verifier = new BestPossAndExtViewZkVerifier(
        ZK_ADDR, clusterName, errStates);
    try {
      boolean result = ClusterStateVerifier.verifyByPolling(
          verifier);
      Assert.assertTrue(result);
    } finally {
      verifier.close();
    }

    Map<String, Set<String>> errorStateMap = new HashMap<String, Set<String>>();
    errorStateMap.put(errPartition, TestHelper.setOf(masterOfPartition0));

    // verify "TestDB0_0", masterOfPartition0 is in ERROR state
    TestHelper.verifyState(clusterName, ZK_ADDR, errorStateMap, "ERROR");

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testParticipantIncompatibleWithBatchMsg() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // enable batch message
    // --addResourceProperty <clusterName resourceName propertyName propertyValue>
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--addResourceProperty", clusterName, "TestDB0",
        HelixPropertyAttribute.BATCH_MESSAGE_MODE.toString(), "true"
    });

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    // register a message listener so we know how many message generated
    TestZkChildListener listener = new TestZkChildListener();
    _gZkClient.subscribeChildChanges(keyBuilder.messages("localhost_12918").getPath(), listener);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // pause controller
    // --enableCluster <clusterName true/false>
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--enableCluster", clusterName, "false"
    });

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    // change localhost_12918 version to 0.5, so batch-message-mode will be ignored
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance("localhost_12918"));
    liveInstance.setHelixVersion("0.5");
    accessor.setProperty(keyBuilder.liveInstance("localhost_12918"), liveInstance);

    // resume controller
    // --enableCluster <clusterName true/false>
    ClusterSetup.processCommandLineArgs(new String[] {
        "--zkSvr", ZK_ADDR, "--enableCluster", clusterName, "true"
    });

    BestPossAndExtViewZkVerifier verifier = new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName);
    try {
      boolean result = ClusterStateVerifier
          .verifyByZkCallback(verifier);
      Assert.assertTrue(result);
      Assert.assertTrue(listener._maxNumberOfChildren > 16,
          "Should see more than 16 messages at the same time (32 O->S and 32 S->M)");
    } finally {
      verifier.close();
    }

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
