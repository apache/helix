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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixManager;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.strategy.DefaultTwoStateStrategy;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.store.PropertyJsonSerializer;
import org.apache.helix.store.PropertyStoreException;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.TestCommand;
import org.apache.helix.tools.TestCommand.CommandType;
import org.apache.helix.tools.TestExecutor;
import org.apache.helix.tools.TestExecutor.ZnodePropertyType;
import org.apache.helix.tools.TestTrigger;
import org.apache.helix.tools.ZnodeOpArg;
import org.apache.log4j.Logger;
import org.testng.Assert;

public class TestDriver {
  private static Logger LOG = Logger.getLogger(TestDriver.class);

  // private static final String CLUSTER_PREFIX = "TestDriver";
  private static final String STATE_MODEL = "MasterSlave";
  private static final String TEST_DB_PREFIX = "TestDB";
  private static final int START_PORT = 12918;
  private static final String CONTROLLER_PREFIX = "controller";
  private static final String PARTICIPANT_PREFIX = "localhost";
  private static final Random RANDOM = new Random();
  private static final PropertyJsonSerializer<ZNRecord> SERIALIZER =
      new PropertyJsonSerializer<ZNRecord>(ZNRecord.class);

  private static final Map<String, TestInfo> _testInfoMap =
      new ConcurrentHashMap<String, TestInfo>();

  public static class TestInfo {
    public final ZkClient _zkClient;
    public final String _clusterName;
    public final int _numDb;
    public final int _numPartitionsPerDb;
    public final int _numNode;
    public final int _replica;

    public final Map<String, HelixManager> _managers =
        new ConcurrentHashMap<String, HelixManager>();

    public TestInfo(String clusterName, ZkClient zkClient, int numDb, int numPartitionsPerDb,
        int numNode, int replica) {
      this._clusterName = clusterName;
      this._zkClient = zkClient;
      this._numDb = numDb;
      this._numPartitionsPerDb = numPartitionsPerDb;
      this._numNode = numNode;
      this._replica = replica;
    }
  }

  public static TestInfo getTestInfo(String uniqClusterName) {
    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "Cluster hasn't been setup for " + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqClusterName);
    return testInfo;
  }

  public static void setupClusterWithoutRebalance(String uniqClusterName, String zkAddr,
      int numResources, int numPartitionsPerResource, int numInstances, int replica)
      throws Exception {
    setupCluster(uniqClusterName, zkAddr, numResources, numPartitionsPerResource, numInstances,
        replica, false);
  }

  public static void setupCluster(String uniqClusterName, String zkAddr, int numResources,
      int numPartitionsPerResource, int numInstances, int replica) throws Exception {
    setupCluster(uniqClusterName, zkAddr, numResources, numPartitionsPerResource, numInstances,
        replica, true);
  }

  public static void setupCluster(String uniqClusterName, String zkAddr, int numResources,
      int numPartitionsPerResource, int numInstances, int replica, boolean doRebalance)
      throws Exception {
    ZkClient zkClient = new ZkClient(zkAddr);
    zkClient.setZkSerializer(new ZNRecordSerializer());

    // String clusterName = CLUSTER_PREFIX + "_" + uniqClusterName;
    String clusterName = uniqClusterName;
    if (zkClient.exists("/" + clusterName)) {
      LOG.warn("test cluster already exists:" + clusterName + ", test name:" + uniqClusterName
          + " is not unique or test has been run without cleaning up zk; deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    if (_testInfoMap.containsKey(uniqClusterName)) {
      LOG.warn("test info already exists:" + uniqClusterName
          + " is not unique or test has been run without cleaning up test info map; removing it");
      _testInfoMap.remove(uniqClusterName);
    }
    TestInfo testInfo =
        new TestInfo(clusterName, zkClient, numResources, numPartitionsPerResource, numInstances,
            replica);
    _testInfoMap.put(uniqClusterName, testInfo);

    ClusterSetup setupTool = new ClusterSetup(zkAddr);
    setupTool.addCluster(clusterName, true);

    for (int i = 0; i < numInstances; i++) {
      int port = START_PORT + i;
      setupTool.addInstanceToCluster(clusterName, PARTICIPANT_PREFIX + "_" + port);
    }

    for (int i = 0; i < numResources; i++) {
      String dbName = TEST_DB_PREFIX + i;
      setupTool.addResourceToCluster(clusterName, dbName, numPartitionsPerResource, STATE_MODEL);
      if (doRebalance) {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);

        // String idealStatePath = "/" + clusterName + "/" +
        // PropertyType.IDEALSTATES.toString() + "/"
        // + dbName;
        // ZNRecord idealState = zkClient.<ZNRecord> readData(idealStatePath);
        // testInfo._idealStateMap.put(dbName, idealState);
      }
    }
  }

  /**
   * starting a dummy participant with a given id
   * @param uniqueTestName
   * @param instanceId
   */
  public static void startDummyParticipant(String zkAddr, String uniqClusterName, int instanceId) throws Exception {
    startDummyParticipants(zkAddr, uniqClusterName, new int[] {
      instanceId
    });
  }

  public static void startDummyParticipants(String zkAddr, String uniqClusterName, int[] instanceIds)
      throws Exception {
    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqClusterName);
    String clusterName = testInfo._clusterName;

    for (int id : instanceIds) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + id);

      // if (testInfo._startCMResultMap.containsKey(instanceName)) {
      if (testInfo._managers.containsKey(instanceName)) {
        LOG.warn("Dummy participant:" + instanceName + " has already started; skip starting it");
      } else {
        // StartCMResult result = TestHelper.startDummyProcess(ZK_ADDR, clusterName, instanceName);
        MockParticipant participant =
            new MockParticipant(zkAddr, clusterName, instanceName);
        participant.syncStart();
        testInfo._managers.put(instanceName, participant);
        // testInfo._instanceStarted.countDown();
      }
    }
  }

  public static void startController(String zkAddr, String uniqClusterName) throws Exception {
    startController(zkAddr, uniqClusterName, new int[] {
      0
    });
  }

  public static void startController(String zkAddr, String uniqClusterName, int[] nodeIds) throws Exception {
    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqClusterName);
    String clusterName = testInfo._clusterName;

    for (int id : nodeIds) {
      String controllerName = CONTROLLER_PREFIX + "_" + id;
      if (testInfo._managers.containsKey(controllerName)) {
        LOG.warn("Controller:" + controllerName + " has already started; skip starting it");
      } else {
        MockController controller =
            new MockController(zkAddr, clusterName, controllerName);
        controller.syncStart();
        testInfo._managers.put(controllerName, controller);
      }
    }
  }

  public static void verifyCluster(String zkAddr, String uniqClusterName, long beginTime, long timeout)
      throws Exception {
    Thread.sleep(beginTime);

    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqClusterName);
    String clusterName = testInfo._clusterName;

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            zkAddr, clusterName), timeout);
    Assert.assertTrue(result);
  }

  public static void stopCluster(String uniqClusterName) throws Exception {
    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }
    TestInfo testInfo = _testInfoMap.remove(uniqClusterName);

    // stop controller first
    for (String instanceName : testInfo._managers.keySet()) {
      if (instanceName.startsWith(CONTROLLER_PREFIX)) {
        MockController controller =
            (MockController) testInfo._managers.get(instanceName);
        controller.syncStop();
      }
    }

    Thread.sleep(1000);

    for (String instanceName : testInfo._managers.keySet()) {
      if (!instanceName.startsWith(CONTROLLER_PREFIX)) {
        MockParticipant participant =
            (MockParticipant) testInfo._managers.get(instanceName);
        participant.syncStop();
      }
    }

    testInfo._zkClient.close();
  }

  public static void stopDummyParticipant(String uniqClusterName, long beginTime, int instanceId)
      throws Exception {
    if (!_testInfoMap.containsKey(uniqClusterName)) {

      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new Exception(errMsg);
    }

    TestInfo testInfo = _testInfoMap.get(uniqClusterName);

    String failHost = PARTICIPANT_PREFIX + "_" + (START_PORT + instanceId);
    MockParticipant participant =
        (MockParticipant) testInfo._managers.remove(failHost);

    // TODO need sync
    if (participant == null) {
      String errMsg = "Dummy participant:" + failHost + " seems not running";
      LOG.error(errMsg);
    } else {
      // System.err.println("try to stop participant: " +
      // result._manager.getInstanceName());
      // NodeOpArg arg = new NodeOpArg(result._manager, result._thread);
      // TestCommand command = new TestCommand(CommandType.STOP, new TestTrigger(beginTime), arg);
      // List<TestCommand> commandList = new ArrayList<TestCommand>();
      // commandList.add(command);
      // TestExecutor.executeTestAsync(commandList, ZK_ADDR);
      participant.syncStop();
    }
  }

  public static void setIdealState(String zkAddr, String uniqClusterName, long beginTime, int percentage)
      throws Exception {
    if (!_testInfoMap.containsKey(uniqClusterName)) {
      String errMsg = "test cluster hasn't been setup:" + uniqClusterName;
      throw new IllegalArgumentException(errMsg);
    }
    TestInfo testInfo = _testInfoMap.get(uniqClusterName);
    String clusterName = testInfo._clusterName;
    List<String> instanceNames = new ArrayList<String>();

    for (int i = 0; i < testInfo._numNode; i++) {
      int port = START_PORT + i;
      instanceNames.add(PARTICIPANT_PREFIX + "_" + port);
    }

    List<TestCommand> commandList = new ArrayList<TestCommand>();
    for (int i = 0; i < testInfo._numDb; i++) {
      String dbName = TEST_DB_PREFIX + i;
      ZNRecord destIS =
          DefaultTwoStateStrategy.calculateIdealState(instanceNames, testInfo._numPartitionsPerDb,
              testInfo._replica - 1, dbName, "MASTER", "SLAVE");
      // destIS.setId(dbName);
      destIS.setSimpleField(IdealStateProperty.REBALANCE_MODE.toString(),
          RebalanceMode.CUSTOMIZED.toString());
      destIS.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(),
          Integer.toString(testInfo._numPartitionsPerDb));
      destIS.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), STATE_MODEL);
      destIS.setSimpleField(IdealStateProperty.REPLICAS.toString(), "" + testInfo._replica);
      // String idealStatePath = "/" + clusterName + "/" +
      // PropertyType.IDEALSTATES.toString() + "/"
      // + TEST_DB_PREFIX + i;
      ZNRecord initIS = new ZNRecord(dbName); // _zkClient.<ZNRecord>
                                              // readData(idealStatePath);
      initIS.setSimpleField(IdealStateProperty.REBALANCE_MODE.toString(),
          RebalanceMode.CUSTOMIZED.toString());
      initIS.setSimpleField(IdealStateProperty.NUM_PARTITIONS.toString(),
          Integer.toString(testInfo._numPartitionsPerDb));
      initIS.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), STATE_MODEL);
      initIS.setSimpleField(IdealStateProperty.REPLICAS.toString(), "" + testInfo._replica);
      int totalStep = calcuateNumTransitions(initIS, destIS);
      // LOG.info("initIS:" + initIS);
      // LOG.info("destIS:" + destIS);
      // LOG.info("totalSteps from initIS to destIS:" + totalStep);
      // System.out.println("initIS:" + initIS);
      // System.out.println("destIS:" + destIS);

      ZNRecord nextIS;
      int step = totalStep * percentage / 100;
      System.out.println("Resource:" + dbName + ", totalSteps from initIS to destIS:" + totalStep
          + ", walk " + step + " steps(" + percentage + "%)");
      nextIS = nextIdealState(initIS, destIS, step);
      // testInfo._idealStateMap.put(dbName, nextIS);
      String idealStatePath =
          PropertyPathConfig.getPath(PropertyType.IDEALSTATES, clusterName, TEST_DB_PREFIX + i);
      ZnodeOpArg arg = new ZnodeOpArg(idealStatePath, ZnodePropertyType.ZNODE, "+", nextIS);
      TestCommand command = new TestCommand(CommandType.MODIFY, new TestTrigger(beginTime), arg);
      commandList.add(command);
    }

    TestExecutor.executeTestAsync(commandList, zkAddr);

  }

  private static List<String[]> findAllUnfinishPairs(ZNRecord cur, ZNRecord dest) {
    // find all (host, resource) pairs that haven't reached destination state
    List<String[]> list = new ArrayList<String[]>();
    Map<String, Map<String, String>> map = dest.getMapFields();
    for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
      String partitionName = entry.getKey();
      Map<String, String> hostMap = entry.getValue();
      for (Map.Entry<String, String> hostEntry : hostMap.entrySet()) {
        String host = hostEntry.getKey();
        String destState = hostEntry.getValue();
        Map<String, String> curHostMap = cur.getMapField(partitionName);

        String curState = null;
        if (curHostMap != null) {
          curState = curHostMap.get(host);
        }

        String[] pair = new String[3];
        if (curState == null) {
          if (destState.equalsIgnoreCase("SLAVE")) {
            pair[0] = new String(partitionName);
            pair[1] = new String(host);
            pair[2] = new String("1"); // number of transitions required
            list.add(pair);
          } else if (destState.equalsIgnoreCase("MASTER")) {
            pair[0] = new String(partitionName);
            pair[1] = new String(host);
            pair[2] = new String("2"); // number of transitions required
            list.add(pair);
          }
        } else {
          if (curState.equalsIgnoreCase("SLAVE") && destState.equalsIgnoreCase("MASTER")) {
            pair[0] = new String(partitionName);
            pair[1] = new String(host);
            pair[2] = new String("1"); // number of transitions required
            list.add(pair);
          }
        }
      }
    }
    return list;
  }

  private static int calcuateNumTransitions(ZNRecord start, ZNRecord end) {
    int totalSteps = 0;
    List<String[]> list = findAllUnfinishPairs(start, end);
    for (String[] pair : list) {
      totalSteps += Integer.parseInt(pair[2]);
    }
    return totalSteps;
  }

  private static ZNRecord nextIdealState(final ZNRecord cur, final ZNRecord dest, final int steps)
      throws PropertyStoreException {
    // get a deep copy
    ZNRecord next = SERIALIZER.deserialize(SERIALIZER.serialize(cur));
    List<String[]> list = findAllUnfinishPairs(cur, dest);

    // randomly pick up pairs that haven't reached destination state and
    // progress
    for (int i = 0; i < steps; i++) {
      int randomInt = RANDOM.nextInt(list.size());
      String[] pair = list.get(randomInt);
      String curState = null;
      Map<String, String> curHostMap = next.getMapField(pair[0]);
      if (curHostMap != null) {
        curState = curHostMap.get(pair[1]);
      }
      final String destState = dest.getMapField(pair[0]).get(pair[1]);

      // TODO generalize it using state-model
      if (curState == null && destState != null) {
        Map<String, String> hostMap = next.getMapField(pair[0]);
        if (hostMap == null) {
          hostMap = new HashMap<String, String>();
        }
        hostMap.put(pair[1], "SLAVE");
        next.setMapField(pair[0], hostMap);
      } else if (curState.equalsIgnoreCase("SLAVE") && destState != null
          && destState.equalsIgnoreCase("MASTER")) {
        next.getMapField(pair[0]).put(pair[1], "MASTER");
      } else {
        LOG.error("fail to calculate the next ideal state");
      }
      curState = next.getMapField(pair[0]).get(pair[1]);
      if (curState != null && curState.equalsIgnoreCase(destState)) {
        list.remove(randomInt);
      }
    }

    LOG.info("nextIS:" + next);
    return next;
  }
}
