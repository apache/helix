package org.apache.helix;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.I0Itec.zkclient.IZkStateListener;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope.ConfigScopeProperty;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.Attributes;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterStateVerifier.ZkVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.util.ZKClientPool;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;

// TODO merge code with ZkIntegrationTestBase
public class ZkUnitTestBase {
  private static Logger LOG = Logger.getLogger(ZkUnitTestBase.class);
  protected static ZkServer _zkServer = null;
  protected static ZkClient _gZkClient;

  public static final String ZK_ADDR = "localhost:2185";
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";

  @BeforeSuite(alwaysRun = true)
  public void beforeSuite() throws Exception {
    _zkServer = TestHelper.startZkServer(ZK_ADDR);
    AssertJUnit.assertTrue(_zkServer != null);
    ZKClientPool.reset();

    // System.out.println("Number of open zkClient before ZkUnitTests: "
    // + ZkClient.getNumberOfConnections());

    _gZkClient = new ZkClient(ZK_ADDR);
    _gZkClient.setZkSerializer(new ZNRecordSerializer());
  }

  @AfterSuite(alwaysRun = true)
  public void afterTest() {
    _gZkClient.close();
    TestHelper.stopZkServer(_zkServer);
    _zkServer = null;

    // System.out.println("Number of open zkClient after ZkUnitTests: "
    // + ZkClient.getNumberOfConnections());

  }

  protected String getShortClassName() {
    String className = this.getClass().getName();
    return className.substring(className.lastIndexOf('.') + 1);
  }

  protected String getCurrentLeader(ZkClient zkClient, String clusterName) {
    String leaderPath = HelixUtil.getControllerPropertyPath(clusterName, PropertyType.LEADER);
    ZNRecord leaderRecord = zkClient.<ZNRecord> readData(leaderPath);
    if (leaderRecord == null) {
      return null;
    }

    String leader = leaderRecord.getSimpleField(PropertyType.LEADER.toString());
    return leader;
  }

  protected void stopCurrentLeader(ZkClient zkClient, String clusterName,
      Map<String, Thread> threadMap, Map<String, HelixManager> managerMap) {
    String leader = getCurrentLeader(zkClient, clusterName);
    Assert.assertTrue(leader != null);
    System.out.println("stop leader:" + leader + " in " + clusterName);
    Assert.assertTrue(leader != null);

    HelixManager manager = managerMap.remove(leader);
    Assert.assertTrue(manager != null);
    manager.disconnect();

    Thread thread = threadMap.remove(leader);
    Assert.assertTrue(thread != null);
    thread.interrupt();

    boolean isNewLeaderElected = false;
    try {
      // Thread.sleep(2000);
      for (int i = 0; i < 5; i++) {
        Thread.sleep(1000);
        String newLeader = getCurrentLeader(zkClient, clusterName);
        if (!newLeader.equals(leader)) {
          isNewLeaderElected = true;
          System.out.println("new leader elected: " + newLeader + " in " + clusterName);
          break;
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (isNewLeaderElected == false) {
      System.out.println("fail to elect a new leader elected in " + clusterName);
    }
    AssertJUnit.assertTrue(isNewLeaderElected);
  }

  public void verifyInstance(ZkClient zkClient, String clusterName, String instance,
      boolean wantExists) {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
    String instanceConfigsPath =
        PropertyPathConfig.getPath(PropertyType.CONFIGS, clusterName,
            ConfigScopeProperty.PARTICIPANT.toString());
    String instanceConfigPath = instanceConfigsPath + "/" + instance;
    String instancePath = HelixUtil.getInstancePath(clusterName, instance);
    AssertJUnit.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
    AssertJUnit.assertEquals(wantExists, zkClient.exists(instancePath));
  }

  public void verifyResource(ZkClient zkClient, String clusterName, String resource,
      boolean wantExists) {
    String resourcePath = HelixUtil.getIdealStatePath(clusterName) + "/" + resource;
    AssertJUnit.assertEquals(wantExists, zkClient.exists(resourcePath));
  }

  public void verifyEnabled(ZkClient zkClient, String clusterName, String instance,
      boolean wantEnabled) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instance));
    AssertJUnit.assertEquals(wantEnabled, config.getInstanceEnabled());
  }

  public void verifyReplication(ZkClient zkClient, String clusterName, String resource, int repl) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resource));
    for (String partitionName : idealState.getPartitionSet()) {
      if (idealState.getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
        AssertJUnit.assertEquals(repl, idealState.getPreferenceList(partitionName).size());
      } else if (idealState.getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
        AssertJUnit.assertEquals(repl, idealState.getInstanceStateMap(partitionName).size());
      }
    }
  }

  protected void simulateSessionExpiry(ZkConnection zkConnection) throws IOException,
      InterruptedException {
    ZooKeeper oldZookeeper = zkConnection.getZookeeper();
    LOG.info("Old sessionId = " + oldZookeeper.getSessionId());

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("In New connection, process event:" + event);
      }
    };

    ZooKeeper newZookeeper =
        new ZooKeeper(zkConnection.getServers(), oldZookeeper.getSessionTimeout(), watcher,
            oldZookeeper.getSessionId(), oldZookeeper.getSessionPasswd());
    LOG.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    oldZookeeper = zkConnection.getZookeeper();
    LOG.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }

  protected void simulateSessionExpiry(ZkClient zkClient) throws IOException, InterruptedException {
    IZkStateListener listener = new IZkStateListener() {
      @Override
      public void handleStateChanged(KeeperState state) throws Exception {
        LOG.info("In Old connection, state changed:" + state);
      }

      @Override
      public void handleNewSession() throws Exception {
        LOG.info("In Old connection, new session");
      }
    };
    zkClient.subscribeStateChanges(listener);
    ZkConnection connection = ((ZkConnection) zkClient.getConnection());
    ZooKeeper oldZookeeper = connection.getZookeeper();
    LOG.info("Old sessionId = " + oldZookeeper.getSessionId());

    Watcher watcher = new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        LOG.info("In New connection, process event:" + event);
      }
    };

    ZooKeeper newZookeeper =
        new ZooKeeper(connection.getServers(), oldZookeeper.getSessionTimeout(), watcher,
            oldZookeeper.getSessionId(), oldZookeeper.getSessionPasswd());
    LOG.info("New sessionId = " + newZookeeper.getSessionId());
    // Thread.sleep(3000);
    newZookeeper.close();
    Thread.sleep(10000);
    connection = (ZkConnection) zkClient.getConnection();
    oldZookeeper = connection.getZookeeper();
    LOG.info("After session expiry sessionId = " + oldZookeeper.getSessionId());
  }

  protected void setupStateModel(String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    accessor.setProperty(keyBuilder.stateModelDef(masterSlave.getId()), masterSlave);

    StateModelDefinition leaderStandby =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby());
    accessor.setProperty(keyBuilder.stateModelDef(leaderStandby.getId()), leaderStandby);

    StateModelDefinition onlineOffline =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
    accessor.setProperty(keyBuilder.stateModelDef(onlineOffline.getId()), onlineOffline);

  }

  protected List<IdealState> setupIdealState(String clusterName, int[] nodes, String[] resources,
      int partitions, int replicas) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i : nodes) {
      instances.add("localhost_" + i);
    }

    for (String resourceName : resources) {
      IdealState idealState = new IdealState(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++) {
          int n = nodes[(p + r) % nodes.length];
          value.add("localhost_" + n);
        }
        idealState.getRecord().setListField(resourceName + "_" + p, value);
      }

      idealState.setReplicas(Integer.toString(replicas));
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);
      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }

  protected void setupLiveInstances(String clusterName, int[] liveInstances) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < liveInstances.length; i++) {
      String instance = "localhost_" + liveInstances[i];
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setSessionId("session_" + liveInstances[i]);
      liveInstance.setHelixVersion("0.0.0");
      accessor.setProperty(keyBuilder.liveInstance(instance), liveInstance);
    }
  }

  protected void setupInstances(String clusterName, int[] instances) {
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    for (int i = 0; i < instances.length; i++) {
      String instance = "localhost_" + instances[i];
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + instances[i]);
      instanceConfig.setInstanceEnabled(true);
      admin.addInstance(clusterName, instanceConfig);
    }
  }

  protected void runPipeline(ClusterEvent event, Pipeline pipeline) {
    try {
      pipeline.handle(event);
      pipeline.finish();
    } catch (Exception e) {
      LOG.error("Exception while executing pipeline:" + pipeline
          + ". Will not continue to next pipeline", e);
    }
  }

  protected void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  protected Message createMessage(MessageType type, String msgId, String fromState, String toState,
      String resourceName, String tgtName) {
    Message msg = new Message(type.toString(), msgId);
    msg.setFromState(fromState);
    msg.setToState(toState);
    msg.getRecord().setSimpleField(Attributes.RESOURCE_NAME.toString(), resourceName);
    msg.setTgtName(tgtName);
    return msg;
  }

  /**
   * Poll for the existence (or lack thereof) of a specific Helix property
   * @param clazz the HelixProeprty subclass
   * @param accessor connected HelixDataAccessor
   * @param key the property key to look up
   * @param shouldExist true if the property should exist, false otherwise
   * @return the property if found, or null if it does not exist
   */
  protected <T extends HelixProperty> T pollForProperty(Class<T> clazz, HelixDataAccessor accessor,
      PropertyKey key, boolean shouldExist) throws InterruptedException {
    final int POLL_TIMEOUT = 5000;
    final int POLL_INTERVAL = 50;
    T property = accessor.getProperty(key);
    int timeWaited = 0;
    while (((shouldExist && property == null) || (!shouldExist && property != null))
        && timeWaited < POLL_TIMEOUT) {
      Thread.sleep(POLL_INTERVAL);
      timeWaited += POLL_INTERVAL;
      property = accessor.getProperty(key);
    }
    return property;
  }

  /**
   * Ensures that external view and current state are empty
   */
  protected static class EmptyZkVerifier implements ZkVerifier {
    private final String _clusterName;
    private final String _resourceName;
    private final ZkClient _zkClient;

    /**
     * Instantiate the verifier
     * @param clusterName the cluster to verify
     * @param resourceName the resource to verify
     */
    public EmptyZkVerifier(String clusterName, String resourceName) {
      _clusterName = clusterName;
      _resourceName = resourceName;
      _zkClient = ZKClientPool.getZkClient(ZK_ADDR);
    }

    @Override
    public boolean verify() {
      BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_zkClient);
      HelixDataAccessor accessor = new ZKHelixDataAccessor(_clusterName, baseAccessor);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      ExternalView externalView = accessor.getProperty(keyBuilder.externalView(_resourceName));

      // verify external view empty
      if (externalView != null) {
        for (String partition : externalView.getPartitionSet()) {
          Map<String, String> stateMap = externalView.getStateMap(partition);
          if (stateMap != null && !stateMap.isEmpty()) {
            LOG.error("External view not empty for " + partition);
            return false;
          }
        }
      }

      // verify current state empty
      List<String> liveParticipants = accessor.getChildNames(keyBuilder.liveInstances());
      for (String participant : liveParticipants) {
        List<String> sessionIds = accessor.getChildNames(keyBuilder.sessions(participant));
        for (String sessionId : sessionIds) {
          CurrentState currentState =
              accessor.getProperty(keyBuilder.currentState(participant, sessionId, _resourceName));
          Map<String, String> partitionStateMap = currentState.getPartitionStateMap();
          if (partitionStateMap != null && !partitionStateMap.isEmpty()) {
            LOG.error("Current state not empty for " + participant);
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public ZkClient getZkClient() {
      return _zkClient;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }
  }
}
