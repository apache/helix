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
 *   http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.store.ZNRecordJsonSerializer;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHelixAdminCli extends ZkTestBase {
  @Test
  public void testAddCluster() throws Exception {
    String command = "--zkSvr " + _zkaddr + " -addCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // malformed cluster name
    command = "--zkSvr " + _zkaddr + " -addCluster /ClusterTest";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("ClusterSetup should fail since /ClusterTest is not a valid name");
    } catch (Exception e) {
      // OK
    }

    // Add the grand cluster
    // " is ignored by zk
    command = "--zkSvr " + _zkaddr + " -addCluster \"Klazt3rz";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "--zkSvr " + _zkaddr + " -addCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // Add already exist cluster
    command = "--zkSvr " + _zkaddr + " -addCluster clusterTest";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("ClusterSetup should fail since clusterTest already exists");
    } catch (Exception e) {
      // OK
    }

    // make sure clusters are properly setup
    Assert.assertTrue(ZKUtil.isClusterSetup("Klazt3rz", _zkclient));
    Assert.assertTrue(ZKUtil.isClusterSetup("clusterTest", _zkclient));
    Assert.assertTrue(ZKUtil.isClusterSetup("\\ClusterTest", _zkclient));

    // delete cluster without resource and instance
    command = "-zkSvr localhost:2183 -dropCluster \\ClusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropCluster clusterTest1";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -dropCluster clusterTest";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    Assert.assertFalse(_zkclient.exists("/clusterTest"));
    Assert.assertFalse(_zkclient.exists("/\\ClusterTest"));
    Assert.assertFalse(_zkclient.exists("/clusterTest1"));

    // System.out.println("END test");
  }

  @Test
  public void testAddResource() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

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

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testAddInstance() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    String command = "-zkSvr localhost:2183 -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 0; i < 3; i++) {
      command = "-zkSvr localhost:2183 -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    command =
        "-zkSvr localhost:2183 -addNode " + clusterName
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

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testRebalanceResource() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

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

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testStartCluster() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String grandClusterName = clusterName + "_grand";
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    MockParticipant[] participants = new MockParticipant[n];
    MockMultiClusterController[] controllers = new MockMultiClusterController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);

    // activate clusters
    // wrong grand clusterName
    String command =
        "-zkSvr localhost:2183 -activateCluster " + clusterName + " nonExistGrandCluster true";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("add " + clusterName
          + " to grandCluster should fail since grandCluster doesn't exists");
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
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
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
    Assert.assertTrue(leader.getInstanceName().startsWith("controller_900"));

    boolean verifyResult =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    for (MockMultiClusterController controller : controllers) {
      controller.syncStop();
    }
    for (int i = 0; i < participants.length; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDropAddResource() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String grandClusterName = clusterName + "_grand";
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];
    MockMultiClusterController[] controllers = new MockMultiClusterController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + _zkaddr + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // save ideal state
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
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

    command = "-zkSvr " + _zkaddr + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    boolean verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    command =
        "-zkSvr " + _zkaddr + " -addIdealState " + clusterName + " db_11 " + tmpIdealStateFile;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    IdealState idealState2 = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    Assert.assertTrue(idealState2.getRecord().equals(idealState.getRecord()));

    // clean up
    for (int i = 0; i < controllers.length; i++) {
      controllers[i].syncStop();
    }
    for (int i = 0; i < participants.length; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  private void setupCluster(String clusterName, String grandClusterName, final int n,
      MockParticipant[] participants, MockMultiClusterController[] controllers)
      throws Exception, InterruptedException {
    // add cluster
    String command = "-zkSvr " + _zkaddr + " -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add grand cluster
    command = "-zkSvr " + _zkaddr + " -addCluster " + grandClusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add nodes
    for (int i = 0; i < n; i++) {
      command = "-zkSvr " + _zkaddr + " -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    // add resource
    command = "-zkSvr " + _zkaddr + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // rebalance with key prefix
    command = "-zkSvr " + _zkaddr + " -rebalance " + clusterName + " db_11 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // add nodes to grand cluster
    command =
        "-zkSvr " + _zkaddr + " -addNode " + grandClusterName + " controller:9000;controller:9001";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // start mock nodes
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_123" + i;
      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    // start controller nodes
    for (int i = 0; i < 2; i++) {
      controllers[i] =
          new MockMultiClusterController(_zkaddr, grandClusterName, "controller_900" + i);
      controllers[i].syncStart();
    }

    Thread.sleep(100);
  }

  @Test
  public void testInstanceOperations() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String grandClusterName = clusterName + "_grand";
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];
    MockMultiClusterController[] controllers = new MockMultiClusterController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + _zkaddr + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // drop node should fail if the node is not disabled
    command = "-zkSvr " + _zkaddr + " -dropNode " + clusterName + " localhost:1232";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropNode should fail since the node is not disabled");
    } catch (Exception e) {
      // OK
    }

    // disabled node
    command = "-zkSvr " + _zkaddr + " -enableInstance " + clusterName + " localhost:1232 false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // Cannot dropNode if the node is not disconnected
    command = "-zkSvr " + _zkaddr + " -dropNode " + clusterName + " localhost:1232";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropNode should fail since the node is not disconnected");
    } catch (Exception e) {
      // OK
    }

    // Cannot swapNode if the node is not disconnected
    command =
        "-zkSvr " + _zkaddr + " -swapInstance " + clusterName + " localhost_1232 localhost_12320";
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("swapInstance should fail since the node is not disconnected");
    } catch (Exception e) {
      // OK
    }

    // disconnect localhost_1232
    participants[2].syncStop();

    // add new node then swap instance
    command = "-zkSvr " + _zkaddr + " -addNode " + clusterName + " localhost:12320";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // swap instance. The instance get swapped out should not exist anymore
    command =
        "-zkSvr " + _zkaddr + " -swapInstance " + clusterName + " localhost_1232 localhost_12320";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    String path = accessor.keyBuilder().instanceConfig("localhost_1232").getPath();
    Assert.assertFalse(_zkclient.exists(path), path
        + " should not exist since localhost_1232 has been swapped by localhost_12320");

    // clean up
    for (int i = 0; i < controllers.length; i++) {
      controllers[i].syncStop();
    }
    for (int i = 0; i < participants.length; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testExpandCluster() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String grandClusterName = clusterName + "_grand";
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];
    MockMultiClusterController[] controllers = new MockMultiClusterController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + _zkaddr + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    command =
        "-zkSvr " + _zkaddr + " -addNode " + clusterName
            + " localhost:12331;localhost:12341;localhost:12351;localhost:12361";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -expandCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    MockParticipant[] newParticipants = new MockParticipant[4];
    for (int i = 3; i <= 6; i++) {
      String instanceName = "localhost_123" + i + "1";
      newParticipants[i - 3] = new MockParticipant(_zkaddr, clusterName, instanceName);
      newParticipants[i - 3].syncStart();
    }

    boolean verifyResult =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(verifyResult);

    verifyResult =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(verifyResult);

    // clean up
    for (int i = 0; i < controllers.length; i++) {
      controllers[i].syncStop();
    }
    for (int i = 0; i < participants.length; i++) {
      participants[i].syncStop();
    }
    for (int i = 0; i < newParticipants.length; i++) {
      newParticipants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testDeactivateCluster() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String grandClusterName = clusterName + "_grand";
    final int n = 6;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];
    MockMultiClusterController[] controllers = new MockMultiClusterController[2];
    setupCluster(clusterName, grandClusterName, n, participants, controllers);
    String command =
        "-zkSvr " + _zkaddr + " -activateCluster " + clusterName + " " + grandClusterName + " true";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Thread.sleep(500);

    // deactivate cluster
    command =
        "-zkSvr " + _zkaddr + " -activateCluster " + clusterName + " " + grandClusterName
            + " false";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    String path = accessor.keyBuilder().controllerLeader().getPath();
    for (int i = 0; i < 10; i++) {
      Thread.sleep(1000);
      if (!_zkclient.exists(path)) {
        break;
      }
    }
    Assert.assertFalse(_zkclient.exists(path),
        "leader should be gone after deactivate the cluster");

    command = "-zkSvr " + _zkaddr + " -dropCluster " + clusterName;
    try {
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
      Assert.fail("dropCluster should fail since there are still instances running");
    } catch (Exception e) {
      // OK
    }

    for (int i = 0; i < participants.length; i++) {
      participants[i].syncStop();
    }

    command = "-zkSvr localhost:2183 -dropCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s"));

    for (int i = 0; i < controllers.length; i++) {
      controllers[i].syncStop();
    }
    command = "-zkSvr localhost:2183 -dropCluster " + grandClusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testInstanceGroupTags() throws Exception {

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkclient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    String command = "-zkSvr " + _zkaddr + " -addCluster " + clusterName;
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command = "-zkSvr localhost:2183 -addResource " + clusterName + " db_11 12 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 0; i < 6; i++) {
      command = "-zkSvr " + _zkaddr + " -addNode " + clusterName + " localhost:123" + i;
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    for (int i = 0; i < 2; i++) {
      command =
          "-zkSvr " + _zkaddr + " -addInstanceTag " + clusterName + " localhost_123" + i + "  tag1";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }
    for (int i = 2; i < 6; i++) {
      command =
          "-zkSvr " + _zkaddr + " -addInstanceTag " + clusterName + " localhost_123" + i + "  tag2";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    command =
        "-zkSvr " + _zkaddr + " -rebalance " + clusterName + " db_11 2 -instanceGroupTag tag1";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    IdealState dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    Set<ParticipantId> hosts = new HashSet<ParticipantId>();
    for (PartitionId p : dbIs.getPartitionIdSet()) {
      for (ParticipantId participantId : dbIs.getParticipantStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(participantId.stringify()));
        Assert.assertTrue(config.containsTag("tag1"));
        hosts.add(participantId);
      }
    }
    Assert.assertEquals(hosts.size(), 2);

    command = "-zkSvr " + _zkaddr + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    // re-add and rebalance
    command = "-zkSvr " + _zkaddr + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command =
        "-zkSvr " + _zkaddr + " -rebalance " + clusterName + " db_11 3 -instanceGroupTag tag2";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    hosts = new HashSet<ParticipantId>();
    for (PartitionId p : dbIs.getPartitionIdSet()) {
      for (ParticipantId participantId : dbIs.getParticipantStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(participantId.stringify()));
        Assert.assertTrue(config.containsTag("tag2"));
        hosts.add(participantId);
      }
    }
    Assert.assertEquals(hosts.size(), 4);

    command = "-zkSvr " + _zkaddr + " -dropResource " + clusterName + " db_11 ";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    for (int i = 3; i <= 3; i++) {
      command =
          "-zkSvr " + _zkaddr + " -removeInstanceTag " + clusterName + " localhost_123" + i
              + " tag2";
      ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    }

    // re-add and rebalance
    command = "-zkSvr " + _zkaddr + " -addResource " + clusterName + " db_11 48 MasterSlave";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    command =
        "-zkSvr " + _zkaddr + " -rebalance " + clusterName + " db_11 3 -instanceGroupTag tag2";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    dbIs = accessor.getProperty(accessor.keyBuilder().idealStates("db_11"));
    hosts = new HashSet<ParticipantId>();
    for (PartitionId p : dbIs.getPartitionIdSet()) {
      for (ParticipantId participantId : dbIs.getParticipantStateMap(p).keySet()) {
        InstanceConfig config =
            accessor.getProperty(accessor.keyBuilder().instanceConfig(participantId.stringify()));
        Assert.assertTrue(config.containsTag("tag2"));
        hosts.add(participantId);
      }
    }
    Assert.assertEquals(hosts.size(), 3);

    // rebalance with key prefix
    command = "-zkSvr " + _zkaddr + " -rebalance " + clusterName + " db_11 2 -key alias";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
