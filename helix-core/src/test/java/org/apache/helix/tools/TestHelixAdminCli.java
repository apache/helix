package org.apache.helix.tools;

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

import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterDistributedController;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.ZNRecordJsonSerializer;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class TestHelixAdminCli extends ZkTestBase {
  private static final long SLEEP_DURATION = 1000L;
  private String clusterName = TestHelper.getTestClassName();
  private String grandClusterName = clusterName + "_grand";

  @AfterMethod
  public void endTest(Method testMethod, ITestContext testContext) {
    try {
      deleteCluster(clusterName);
      deleteCluster(grandClusterName);
    } catch (Exception ignored) {
      // OK
    }
    super.endTest(testMethod, testContext);
  }

  @Test
  public void testAddCluster() throws Exception {
    String command = "--zkSvr localhost:2183 -addCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // malformed cluster name
    command = "--zkSvr localhost:2183 -addCluster /ClusterTest";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("ClusterSetup should fail since /ClusterTest is not a valid name");
    } catch (Exception e) {
      // OK
    }

    // Add the grand cluster
    // " is ignored by zk
    command = "--zkSvr localhost:2183 -addCluster \"Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "--zkSvr localhost:2183 -addCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // Add already exist cluster
    command = "--zkSvr localhost:2183 -addCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // make sure clusters are properly setup
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", _gZkClient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", _gZkClient));

    // delete cluster without resource and instance
    command = "-zkSvr localhost:2183 -dropCluster \"Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    Assert.assertFalse(_gZkClient.exists("/Klazt3rz"));
    Assert.assertFalse(_gZkClient.exists("/clusterTest"));
    Assert.assertFalse(_gZkClient.exists("/\\ClusterTest"));
    Assert.assertFalse(_gZkClient.exists("/clusterTest1"));
  }

  @Test(dependsOnMethods = "testAddCluster")
  public void testAddResource() throws Exception {
    String command = "-zkSvr localhost:2183 -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_22 144 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_11 44 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // Add duplicate resource
    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_22 55 OnlineOffline";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("ClusterSetup should fail since resource db_22 already exists");
    } catch (Exception e) {
      // OK
    }

    // drop resource now
    command = "-zkSvr localhost:2183 -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
  }

  @Test(dependsOnMethods = "testAddResource")
  public void testAddInstance() throws Exception {
    String command = "-zkSvr localhost:2183 -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 0; i < 3; i++) {
      command = "-zkSvr localhost:2183 -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    command = "-zkSvr localhost:2183 -addNode " + clusterName
        + " localhost:1233;localhost:1234;localhost:1235;localhost:1236";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // delete enabled node
    command = "-zkSvr localhost:2183 -dropNode " + clusterName + " localhost:1236";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("delete node localhost:1236 should fail since it's not disabled");
    } catch (Exception e) {
      // OK
    }

    // delete non-exist node
    command = "-zkSvr localhost:2183 -dropNode " + clusterName + " localhost:12367";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("delete node localhost:1237 should fail since it doesn't exist");
    } catch (Exception e) {
      // OK
    }

    // disable node
    command = "-zkSvr localhost:2183 -enableInstance " + clusterName + " localhost:1236 false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropNode " + clusterName + " localhost:1236";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add a duplicated host
    command = "-zkSvr localhost:2183 -addNode " + clusterName + " localhost:1234";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("add node localhost:1234 should fail since it already exists");
    } catch (Exception e) {
      // OK
    }
  }

  @Test(dependsOnMethods = "testAddInstance")
  public void testRebalanceResource() throws Exception {
    String command = "-zkSvr localhost:2183 -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_11 12 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 0; i < 6; i++) {
      command = "-zkSvr localhost:2183 -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    command = "-zkSvr localhost:2183 -rebalance " + clusterName + " db_11 3";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // re-add and rebalance
    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -rebalance " + clusterName + " db_11 3";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // rebalance with key prefix
    command = "-zkSvr localhost:2183 -rebalance " + clusterName + " db_11 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
  }

  @Test(dependsOnMethods = "testRebalanceResource")
  public void testStartCluster() throws Exception {
    final int n = 6;

    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterDistributedController[] controllers = new ClusterDistributedController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);

    // activate clusters
    // wrong grand clusterName
    String command =
        "-zkSvr localhost:2183 -activateCluster " + clusterName + " nonExistGrandCluster true";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail(
          "add " + clusterName + " to grandCluster should fail since grandCluster doesn't exists");
    } catch (Exception e) {
      // OK
    }

    // wrong cluster name
    command =
        "-zkSvr localhost:2183 -activateCluster nonExistCluster " + grandClusterName + " true";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("add nonExistCluster to " + grandClusterName
          + " should fail since nonExistCluster doesn't exists");
    } catch (Exception e) {
      // OK
    }

    command =
        "-zkSvr localhost:2183 -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // drop a running cluster
    command = "-zkSvr localhost:2183 -dropCluster " + clusterName;
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("drop " + clusterName + " should fail since it's still running");
    } catch (Exception e) {
      // OK
    }

    // verify leader node
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(grandClusterName, baseAccessor);
    LiveInstance controllerLeader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    Assert.assertNotNull(controllerLeader,
        "controllerLeader should be either controller_9000 or controller_9001");
    Assert.assertTrue(controllerLeader.getInstanceName().startsWith("controller_900"));

    accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    LiveInstance leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    for (int i = 0; i < 20; i++) {
      if (leader != null) {
        break;
      }
      Thread.sleep(200);
      leader = accessor.getProperty(accessor.keyBuilder().controllerLeader());
    }
    Assert.assertNotNull(leader);
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));

    boolean verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    for (ClusterDistributedController controller : controllers) {
      controller.syncStop();
    }
    for (ClusterDistributedController controller : controllers) {
      if (controller.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      if (participant.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
  }

  @Test(dependsOnMethods = "testStartCluster")
  public void testDropAddResource() throws Exception {
    final int n = 6;

    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterDistributedController[] controllers = new ClusterDistributedController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + ZK_ADDR + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // save ideal state
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    ZNRecordJsonSerializer serializer = new ZNRecordJsonSerializer();

    String tmpDir = System.getProperty("java.io.tmpdir");
    if (tmpDir == null) {
      tmpDir = "/tmp";
    }
    final String tmpIdealStateFile = tmpDir + "/" + clusterName + "_idealState.log";
    FileWriter fos = new FileWriter(tmpIdealStateFile);
    PrintWriter pw = new PrintWriter(fos);
    pw.write(new String(serializer.serialize(idealState.getRecord())));
    pw.close();

    command = "-zkSvr " + ZK_ADDR + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    boolean verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    command =
        "-zkSvr " + ZK_ADDR + " -addIdealState " + clusterName + " db_11 " + tmpIdealStateFile;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    IdealState idealState2 = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    Assert.assertEquals(idealState.getRecord(), idealState2.getRecord());

    // clean up
    for (ClusterDistributedController controller : controllers) {
      controller.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
  }

  private void setupCluster(String clusterName, String grandClusterName, final int n,
      MockParticipantManager[] participants, ClusterDistributedController[] controllers)
      throws Exception {
    // add cluster
    String command = "-zkSvr " + ZK_ADDR + " -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add grand cluster
    command = "-zkSvr " + ZK_ADDR + " -addCluster " + grandClusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add nodes
    for (int i = 0; i < n; i++) {
      command = "-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    // add resource
    command = "-zkSvr " + ZK_ADDR + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // rebalance with key prefix
    command = "-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db_11 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add nodes to grand cluster
    command =
        "-zkSvr " + ZK_ADDR + " -addNode " + grandClusterName + " controller:9000;controller:9001";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // start mock nodes
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_123" + i;
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    // start controller nodes
    for (int i = 0; i < 2; i++) {
      controllers[i] =
          new ClusterDistributedController(ZK_ADDR, grandClusterName, "controller_900" + i);
      controllers[i].syncStart();
    }

    Thread.sleep(SLEEP_DURATION);
  }

  @Test(dependsOnMethods = "testDropAddResource")
  public void testInstanceOperations() throws Exception {
    final int n = 6;

    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterDistributedController[] controllers = new ClusterDistributedController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + ZK_ADDR + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // drop node should fail if the node is not disabled
    command = "-zkSvr " + ZK_ADDR + " -dropNode " + clusterName + " localhost:1232";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropNode should fail since the node is not disabled");
    } catch (Exception e) {
      // OK
    }

    // disabled node
    command = "-zkSvr " + ZK_ADDR + " -enableInstance " + clusterName + " localhost:1232 false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // Cannot dropNode if the node is not disconnected
    command = "-zkSvr " + ZK_ADDR + " -dropNode " + clusterName + " localhost:1232";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropNode should fail since the node is not disconnected");
    } catch (Exception e) {
      // OK
    }

    // Cannot swapNode if the node is not disconnected
    command =
        "-zkSvr " + ZK_ADDR + " -swapInstance " + clusterName + " localhost_1232 localhost_12320";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("swapInstance should fail since the node is not disconnected");
    } catch (Exception e) {
      // OK
    }

    // disconnect localhost_1232
    participants[2].syncStop();

    // add new node then swap instance
    command = "-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:12320";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // swap instance. The instance get swapped out should not exist anymore
    command =
        "-zkSvr " + ZK_ADDR + " -swapInstance " + clusterName + " localhost_1232 localhost_12320";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    String path = accessor.keyBuilder().instanceConfig("localhost_1232").getPath();
    Assert.assertFalse(_gZkClient.exists(path),
        path + " should not exist since localhost_1232 has been swapped by localhost_12320");

    // clean up
    for (ClusterDistributedController controller : controllers) {
      controller.syncStop();
    }
    for (ClusterDistributedController controller : controllers) {
      if (controller.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      if (participant.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
  }

  @Test(dependsOnMethods = "testInstanceOperations")
  public void testExpandCluster() throws Exception {
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterDistributedController[] controllers = new ClusterDistributedController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + ZK_ADDR + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    command = "-zkSvr " + ZK_ADDR + " -addNode " + clusterName
        + " localhost:12331;localhost:12341;localhost:12351;localhost:12361";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -expandCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    MockParticipantManager[] newParticipants = new MockParticipantManager[4];
    for (int i = 3; i <= 6; i++) {
      String instanceName = "localhost_123" + i + "1";
      newParticipants[i - 3] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      newParticipants[i - 3].syncStart();
    }

    boolean verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new MasterNbInExtViewVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult = ClusterStateVerifier
        .verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR, clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    for (ClusterDistributedController controller : controllers) {
      controller.syncStop();
    }
    for (ClusterDistributedController controller : controllers) {
      if (controller.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      if (participant.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
    for (MockParticipantManager newParticipant : newParticipants) {
      newParticipant.syncStop();
    }
    for (MockParticipantManager newParticipant : newParticipants) {
      if (newParticipant.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }
  }

  @Test(dependsOnMethods = "testExpandCluster")
  public void testDeactivateCluster() throws Exception {
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[n];
    ClusterDistributedController[] controllers = new ClusterDistributedController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + ZK_ADDR + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // deactivate cluster
    command = "-zkSvr " + ZK_ADDR + " -activateCluster " + clusterName + " " + grandClusterName
        + " false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    final String path = accessor.keyBuilder().controllerLeader().getPath();
    TestHelper.verify(() -> !_gZkClient.exists(path), 10000L);

    Assert.assertFalse(_gZkClient.exists(path),
        "leader should be gone after deactivate the cluster");

    command = "-zkSvr " + ZK_ADDR + " -dropCluster " + clusterName;
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropCluster should fail since there are still instances running");
    } catch (Exception e) {
      // OK
    }

    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    for (MockParticipantManager participant : participants) {
      if (participant.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }

    command = "-zkSvr localhost:2183 -dropCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s"));

    for (ClusterDistributedController controller : controllers) {
      controller.syncStop();
    }
    for (ClusterDistributedController controller : controllers) {
      if (controller.isConnected()) {
        Thread.sleep(SLEEP_DURATION);
      }
    }

    command = "-zkSvr localhost:2183 -dropCluster " + grandClusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
  }

  @Test(dependsOnMethods = "testDeactivateCluster")
  public void testInstanceGroupTags() throws Exception {
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);

    String command = "-zkSvr " + ZK_ADDR + " -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_11 12 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 0; i < 6; i++) {
      command = "-zkSvr " + ZK_ADDR + " -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    for (int i = 0; i < 2; i++) {
      command =
          "-zkSvr " + ZK_ADDR + " -addInstanceTag " + clusterName + " localhost_123" + i + "  tag1";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }
    for (int i = 2; i < 6; i++) {
      command =
          "-zkSvr " + ZK_ADDR + " -addInstanceTag " + clusterName + " localhost_123" + i + "  tag2";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    command =
        "-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db_11 2 -instanceGroupTag tag1";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    IdealState dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    Set<String> hosts = new HashSet<>();
    for (String p : dbIs.getPartitionSet()) {
      for (String hostName : dbIs.getInstanceStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag("tag1"));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 2);

    command = "-zkSvr " + ZK_ADDR + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // re-add and rebalance
    command = "-zkSvr " + ZK_ADDR + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command =
        "-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db_11 3 -instanceGroupTag tag2";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    hosts = new HashSet<>();
    for (String p : dbIs.getPartitionSet()) {
      for (String hostName : dbIs.getInstanceStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag("tag2"));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 4);

    command = "-zkSvr " + ZK_ADDR + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 3; i <= 3; i++) {
      command = "-zkSvr " + ZK_ADDR + " -removeInstanceTag " + clusterName + " localhost_123" + i
          + " tag2";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    // re-add and rebalance
    command = "-zkSvr " + ZK_ADDR + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command =
        "-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db_11 3 -instanceGroupTag tag2";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    hosts = new HashSet<>();
    for (String p : dbIs.getPartitionSet()) {
      for (String hostName : dbIs.getInstanceStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(hostName));
        Assert.assertTrue(config.containsTag("tag2"));
        hosts.add(hostName);
      }
    }
    Assert.assertEquals(hosts.size(), 3);

    // rebalance with key prefix
    command = "-zkSvr " + ZK_ADDR + " -rebalance " + clusterName + " db_11 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
  }
}
