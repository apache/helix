package org.apache.helix.common;

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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.google.common.base.Preconditions;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.rebalancer.ConditionBasedRebalancer;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;

public class ZkTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ZkTestBase.class);
  private static final String MULTI_ZK_PROPERTY_KEY = "multiZk";
  private static final String NUM_ZK_PROPERTY_KEY = "numZk";

  protected static ZkServer _zkServer;
  protected static HelixZkClient _gZkClient;
  protected static ClusterSetup _gSetupTool;
  protected static BaseDataAccessor<ZNRecord> _baseAccessor;
  protected static MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();

  private final Map<String, Map<String, HelixZkClient>> _liveInstanceOwners = new HashMap<>();

  private static final String ZK_PREFIX = "localhost:";
  private static final int ZK_START_PORT = 2183;
  public static final String ZK_ADDR = ZK_PREFIX + ZK_START_PORT;
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";
  protected final String CONTROLLER_PREFIX = "controller";
  protected final String PARTICIPANT_PREFIX = "localhost";
  private static final long MANUAL_GC_PAUSE = 4000L;

  /*
   * Multiple ZK references
   */
  // The following maps hold ZK connect string as keys
  protected static final Map<String, ZkServer> _zkServerMap = new HashMap<>();
  protected static final Map<String, HelixZkClient> _helixZkClientMap = new HashMap<>();
  protected static final Map<String, ClusterSetup> _clusterSetupMap = new HashMap<>();
  protected static final Map<String, BaseDataAccessor> _baseDataAccessorMap = new HashMap<>();

  static public void reportPhysicalMemory() {
    com.sun.management.OperatingSystemMXBean os = (com.sun.management.OperatingSystemMXBean)
        java.lang.management.ManagementFactory.getOperatingSystemMXBean();
    long physicalMemorySize = os.getTotalPhysicalMemorySize();
    System.out.println("************ SYSTEM Physical Memory:"  + physicalMemorySize);

    long MB = 1024 * 1024;
    Runtime runtime = Runtime.getRuntime();
    long free = runtime.freeMemory()/MB;
    long total = runtime.totalMemory()/MB;
    System.out.println("************ total memory:" + total + " free memory:" + free);
  }

  @BeforeSuite
  public void beforeSuite() throws Exception {
    // TODO: use logging.properties file to config java.util.logging.Logger levels
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // Due to ZOOKEEPER-2693 fix, we need to specify whitelist for execute zk commends
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    System.setProperty(SystemPropertyKeys.CONTROLLER_MESSAGE_PURGE_DELAY, "3000");

    // Start in-memory ZooKeepers
    // If multi-ZooKeeper is enabled, start more ZKs. Otherwise, just set up one ZK
    int numZkToStart = 1;
    String multiZkConfig = System.getProperty(MULTI_ZK_PROPERTY_KEY);
    if (multiZkConfig != null && multiZkConfig.equalsIgnoreCase(Boolean.TRUE.toString())) {
      String numZkFromConfig = System.getProperty(NUM_ZK_PROPERTY_KEY);
      if (numZkFromConfig != null) {
        try {
          numZkToStart = Math.max(Integer.parseInt(numZkFromConfig), numZkToStart);
        } catch (Exception e) {
          Assert.fail("Failed to parse the number of ZKs from config!");
        }
      } else {
        Assert.fail("multiZk config is set but numZk config is missing!");
      }
    }

    // Start "numZkFromConfigInt" ZooKeepers
    for (int i = 0; i < numZkToStart; i++) {
      startZooKeeper(i);
    }

    // Set the references for backward-compatibility with a single ZK environment
    _zkServer = _zkServerMap.get(ZK_ADDR);
    _gZkClient = _helixZkClientMap.get(ZK_ADDR);
    _gSetupTool = _clusterSetupMap.get(ZK_ADDR);
    _baseAccessor = _baseDataAccessorMap.get(ZK_ADDR);

    // Clean up all JMX objects
    for (ObjectName mbean : _server.queryNames(null, null)) {
      try {
        _server.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }
  }

  /**
   * Starts an additional in-memory ZooKeeper for testing.
   * @param i index to be added to the ZK port to avoid conflicts
   * @throws Exception
   */
  private static synchronized void startZooKeeper(int i) {
    String zkAddress = ZK_PREFIX + (ZK_START_PORT + i);
    _zkServerMap.computeIfAbsent(zkAddress, ZkTestBase::createZookeeperServer);
    _helixZkClientMap.computeIfAbsent(zkAddress, ZkTestBase::createZkClient);
    _clusterSetupMap.computeIfAbsent(zkAddress, key -> new ClusterSetup(_helixZkClientMap.get(key)));
    _baseDataAccessorMap.computeIfAbsent(zkAddress, key -> new ZkBaseDataAccessor(_helixZkClientMap.get(key)));
  }

  private static ZkServer createZookeeperServer(String zkAddress) {
    try {
      return Preconditions.checkNotNull(TestHelper.startZkServer(zkAddress));
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to start zookeeper server at " + zkAddress, e);
    }
  }

  private static HelixZkClient createZkClient(String zkAddress) {
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());
    return DedicatedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress), clientConfig);
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    // Clean up all JMX objects
    for (ObjectName mbean : _server.queryNames(null, null)) {
      try {
        _server.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }

    synchronized (ZkTestBase.class) {
      // Close all ZK resources
      _baseDataAccessorMap.values().forEach(BaseDataAccessor::close);
      _clusterSetupMap.values().forEach(ClusterSetup::close);
      _helixZkClientMap.values().forEach(HelixZkClient::close);
      _zkServerMap.values().forEach(TestHelper::stopZkServer);
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    cleanupJMXObjects();
    // Giving each test some time to settle (such as gc pause, etc).
    // Note that this is the best effort we could make to stabilize tests, not a complete solution
    Runtime.getRuntime().gc();
    Thread.sleep(MANUAL_GC_PAUSE);
  }

  @BeforeMethod
  public void beforeTest(Method testMethod, ITestContext testContext) {
    testContext.setAttribute("StartTime", System.currentTimeMillis());
  }

  protected void cleanupJMXObjects() throws IOException {
    // Clean up all JMX objects
    for (ObjectName mbean : _server.queryNames(null, null)) {
      try {
        _server.unregisterMBean(mbean);
      } catch (Exception e) {
        // OK
      }
    }
  }

  protected String getShortClassName() {
    return this.getClass().getSimpleName();
  }

  protected String getCurrentLeader(HelixZkClient zkClient, String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
    if (leader == null) {
      return null;
    }
    return leader.getInstanceName();
  }

  protected void enablePersistBestPossibleAssignment(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setPersistBestPossibleAssignment(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enablePersistIntermediateAssignment(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setPersistIntermediateAssignment(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableTopologyAwareRebalance(HelixZkClient zkClient, String clusterName,
      Boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setTopologyAwareEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableDelayRebalanceInCluster(HelixZkClient zkClient, String clusterName,
      boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(enabled);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableDelayRebalanceInInstance(HelixZkClient zkClient, String clusterName,
      String instanceName, boolean enabled) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    InstanceConfig instanceConfig = configAccessor.getInstanceConfig(clusterName, instanceName);
    instanceConfig.setDelayRebalanceEnabled(enabled);
    configAccessor.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  protected void enableDelayRebalanceInCluster(HelixZkClient zkClient, String clusterName,
      boolean enabled, long delay) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setDelayRebalaceEnabled(enabled);
    clusterConfig.setRebalanceDelayTime(delay);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void enableP2PInCluster(String clusterName, ConfigAccessor configAccessor,
      boolean enable) {
    // enable p2p message in cluster.
    if (enable) {
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      clusterConfig.enableP2PMessage(true);
      configAccessor.setClusterConfig(clusterName, clusterConfig);
    } else {
      ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
      clusterConfig.getRecord().getSimpleFields()
          .remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
      configAccessor.setClusterConfig(clusterName, clusterConfig);
    }
  }

  protected void enableP2PInResource(String clusterName, ConfigAccessor configAccessor,
      String dbName, boolean enable) {
    if (enable) {
      ResourceConfig resourceConfig =
          new ResourceConfig.Builder(dbName).setP2PMessageEnabled(true).build();
      configAccessor.setResourceConfig(clusterName, dbName, resourceConfig);
    } else {
      // remove P2P Message in resource config
      ResourceConfig resourceConfig = configAccessor.getResourceConfig(clusterName, dbName);
      if (resourceConfig != null) {
        resourceConfig.getRecord().getSimpleFields()
            .remove(HelixConfigProperty.P2P_MESSAGE_ENABLED.name());
        configAccessor.setResourceConfig(clusterName, dbName, resourceConfig);
      }
    }
  }

  protected void setDelayTimeInCluster(HelixZkClient zkClient, String clusterName, long delay) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setRebalanceDelayTime(delay);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected void setLastOnDemandRebalanceTimeInCluster(HelixZkClient zkClient,
      String clusterName, long lastOnDemandTime) {
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(clusterName);
    clusterConfig.setLastOnDemandRebalanceTimestamp(lastOnDemandTime);
    configAccessor.setClusterConfig(clusterName, clusterConfig);
  }

  protected IdealState createResourceWithDelayedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica, long delay) {
    return createResourceWithDelayedRebalance(clusterName, db, stateModel, numPartition, replica,
        minActiveReplica, delay, AutoRebalanceStrategy.class.getName());
  }

  protected IdealState createResourceWithDelayedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica, long delay,
      String rebalanceStrategy) {
    return createResource(clusterName, db, stateModel, numPartition, replica, minActiveReplica,
        delay, DelayedAutoRebalancer.class.getName(), rebalanceStrategy);
  }

  protected IdealState createResourceWithWagedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica) {
    return createResource(clusterName, db, stateModel, numPartition, replica, minActiveReplica,
        -1, WagedRebalancer.class.getName(), null);
  }

  protected IdealState createResourceWithConditionBasedRebalance(String clusterName, String db,
      String stateModel, int numPartition, int replica, int minActiveReplica,
      String rebalanceStrategy) {
    return createResource(clusterName, db, stateModel, numPartition, replica, minActiveReplica, -1,
        ConditionBasedRebalancer.class.getName(), rebalanceStrategy);
  }

  private IdealState createResource(String clusterName, String db, String stateModel,
      int numPartition, int replica, int minActiveReplica, long delay, String rebalancerClassName,
      String rebalanceStrategy) {
    IdealState idealState =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);
    if (idealState == null) {
      _gSetupTool.addResourceToCluster(clusterName, db, numPartition, stateModel,
          IdealState.RebalanceMode.FULL_AUTO + "", rebalanceStrategy);
    }

    idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);
    idealState.setMinActiveReplicas(minActiveReplica);
    if (!idealState.isDelayRebalanceEnabled()) {
      idealState.setDelayRebalanceEnabled(true);
    }
    if (delay > 0) {
      idealState.setRebalanceDelay(delay);
    }
    idealState.setRebalancerClassName(rebalancerClassName);
    _gSetupTool.getClusterManagementTool().setResourceIdealState(clusterName, db, idealState);
    _gSetupTool.rebalanceStorageCluster(clusterName, db, replica);
    idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);

    return idealState;
  }

  protected IdealState createIdealState(String resourceGroupName, String instanceGroupTag,
      List<String> instanceNames, int numPartition, int replica, String rebalanceMode,
      String stateModelDef) {
    IdealState is = _gSetupTool.createIdealStateForResourceGroup(resourceGroupName,
        instanceGroupTag, numPartition, replica, rebalanceMode, stateModelDef);

    // setup initial partition->instance mapping.
    int nodeIdx = 0;
    int numNode = instanceNames.size();
    assert (numNode >= replica);
    for (int i = 0; i < numPartition; i++) {
      String partitionName = resourceGroupName + "_" + i;
      for (int j = 0; j < replica; j++) {
        is.setPartitionState(partitionName, instanceNames.get((nodeIdx + j) % numNode),
            OnlineOfflineSMD.States.ONLINE.toString());
      }
      nodeIdx++;
    }

    return is;
  }

  protected void createDBInSemiAuto(ClusterSetup clusterSetup, String clusterName, String dbName,
      List<String> preferenceList, String stateModelDef, int numPartition, int replica) {
    clusterSetup.addResourceToCluster(clusterName, dbName, numPartition, stateModelDef,
        IdealState.RebalanceMode.SEMI_AUTO.toString());
    clusterSetup.rebalanceStorageCluster(clusterName, dbName, replica);

    IdealState is =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, dbName);
    for (String p : is.getPartitionSet()) {
      is.setPreferenceList(p, preferenceList);
    }
    clusterSetup.getClusterManagementTool().setResourceIdealState(clusterName, dbName, is);
  }

  /**
   * Validate there should be always minimal active replica and top state replica for each
   * partition.
   * Also make sure there is always some partitions with only active replica count.
   */
  protected void validateMinActiveAndTopStateReplica(IdealState is, ExternalView ev,
      int minActiveReplica, int numNodes) {
    StateModelDefinition stateModelDef =
        BuiltInStateModelDefinitions.valueOf(is.getStateModelDefRef()).getStateModelDefinition();
    String topState = stateModelDef.getStatesPriorityList().get(0);
    int replica = Integer.valueOf(is.getReplicas());

    Map<String, Integer> stateCount = stateModelDef.getStateCountMap(numNodes, replica);
    Set<String> activeStates = stateCount.keySet();

    for (String partition : is.getPartitionSet()) {
      Map<String, String> assignmentMap = ev.getRecord().getMapField(partition);
      Assert.assertNotNull(assignmentMap,
          is.getResourceName() + "'s best possible assignment is null for partition " + partition);
      Assert.assertTrue(!assignmentMap.isEmpty(),
          is.getResourceName() + "'s partition " + partition + " has no best possible map in IS.");

      boolean hasTopState = false;
      int activeReplica = 0;
      for (String state : assignmentMap.values()) {
        if (topState.equalsIgnoreCase(state)) {
          hasTopState = true;
        }
        if (activeStates.contains(state)) {
          activeReplica++;
        }
      }

      if (activeReplica < minActiveReplica) {
        int a = 0;
      }

      Assert.assertTrue(hasTopState, String.format("%s missing %s replica", partition, topState));
      Assert.assertTrue(activeReplica >= minActiveReplica,
          String.format("%s has less active replica %d then required %d", partition, activeReplica,
              minActiveReplica));
    }
  }

  protected void runStage(HelixManager manager, ClusterEvent event, Stage stage) throws Exception {
    event.addAttribute(AttributeName.helixmanager.name(), manager);
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();

    // AbstractAsyncBaseStage will run asynchronously, and it's main logics are implemented in
    // execute() function call
    if (stage instanceof AbstractAsyncBaseStage) {
      ((AbstractAsyncBaseStage) stage).execute(event);
    } else {
      stage.process(event);
    }
    stage.postProcess();
  }

  public void verifyInstance(HelixZkClient zkClient, String clusterName, String instance,
      boolean wantExists) {
    // String instanceConfigsPath = HelixUtil.getConfigPath(clusterName);
    String instanceConfigsPath = PropertyPathBuilder.instanceConfig(clusterName);
    String instanceConfigPath = instanceConfigsPath + "/" + instance;
    String instancePath = PropertyPathBuilder.instance(clusterName, instance);
    Assert.assertEquals(wantExists, zkClient.exists(instanceConfigPath));
    Assert.assertEquals(wantExists, zkClient.exists(instancePath));
  }

  public void verifyResource(HelixZkClient zkClient, String clusterName, String resource,
      boolean wantExists) {
    String resourcePath = PropertyPathBuilder.idealState(clusterName, resource);
    Assert.assertEquals(wantExists, zkClient.exists(resourcePath));
  }

  public void verifyEnabled(HelixZkClient zkClient, String clusterName, String instance,
      boolean wantEnabled) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    InstanceConfig config = accessor.getProperty(keyBuilder.instanceConfig(instance));
    Assert.assertEquals(wantEnabled, config.getInstanceEnabled());
  }

  public void verifyReplication(HelixZkClient zkClient, String clusterName, String resource,
      int repl) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(zkClient));
    Builder keyBuilder = accessor.keyBuilder();

    IdealState idealState = accessor.getProperty(keyBuilder.idealStates(resource));
    for (String partitionName : idealState.getPartitionSet()) {
      if (idealState.getRebalanceMode() == IdealState.RebalanceMode.SEMI_AUTO) {
        Assert.assertEquals(repl, idealState.getPreferenceList(partitionName).size());
      } else if (idealState.getRebalanceMode() == IdealState.RebalanceMode.CUSTOMIZED) {
        Assert.assertEquals(repl, idealState.getInstanceStateMap(partitionName).size());
      }
    }
  }

  protected void setupStateModel(String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
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

  protected Message createMessage(Message.MessageType type, String msgId, String fromState,
      String toState, String resourceName, String tgtName) {
    Message msg = new Message(type.toString(), msgId);
    msg.setFromState(fromState);
    msg.setToState(toState);
    msg.getRecord().setSimpleField(Message.Attributes.RESOURCE_NAME.toString(), resourceName);
    msg.setTgtName(tgtName);
    return msg;
  }

  protected List<IdealState> setupIdealState(String clusterName, int[] nodes, String[] resources,
      int partitions, int replicas) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    List<IdealState> idealStates = new ArrayList<>();
    List<String> instances = new ArrayList<>();
    for (int i : nodes) {
      instances.add("localhost_" + i);
    }

    for (String resourceName : resources) {
      IdealState idealState = new IdealState(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<>();
        for (int r = 0; r < replicas; r++) {
          int n = nodes[(p + r) % nodes.length];
          value.add("localhost_" + n);
        }
        idealState.getRecord().setListField(resourceName + "_" + p, value);
      }

      idealState.setReplicas(Integer.toString(replicas));
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setRebalanceMode(IdealState.RebalanceMode.SEMI_AUTO);
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);
      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }

  @AfterClass
  public void cleanupLiveInstanceOwners() throws InterruptedException {
    String testClassName = this.getShortClassName();
    System.out.println("AfterClass: " + testClassName + " called.");
    for (String cluster : _liveInstanceOwners.keySet()) {
      Map<String, HelixZkClient> clientMap = _liveInstanceOwners.get(cluster);
      for (HelixZkClient client : clientMap.values()) {
        client.close();
      }
      clientMap.clear();
    }
    _liveInstanceOwners.clear();
  }

  protected List<LiveInstance> setupLiveInstances(String clusterName, int[] liveInstances) {
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());

    List<LiveInstance> result = new ArrayList<>();

    for (int i = 0; i < liveInstances.length; i++) {
      String instance = "localhost_" + liveInstances[i];
      _liveInstanceOwners.putIfAbsent(clusterName, new HashMap<>());
      Map<String, HelixZkClient> clientMap = _liveInstanceOwners.get(clusterName);
      clientMap.putIfAbsent(instance, DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR), clientConfig));
      HelixZkClient client = clientMap.get(instance);

          ZKHelixDataAccessor accessor =
          new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(client));
      Builder keyBuilder = accessor.keyBuilder();

      LiveInstance liveInstance = new LiveInstance(instance);
      // Keep setting the session id in the deprecated field for ensure the same behavior as a real participant.
      // Note the participant is doing so for backward compatibility.
      liveInstance.setSessionId(Long.toHexString(client.getSessionId()));
      // Please refer to the version requirement here: helix-core/src/main/resources/cluster-manager-version.properties
      // Ensuring version compatibility can avoid the warning message during test.
      liveInstance.setHelixVersion("0.4");
      accessor.setProperty(keyBuilder.liveInstance(instance), liveInstance);
      result.add(accessor.getProperty(keyBuilder.liveInstance(instance)));
    }
    return result;
  }

  protected void deleteLiveInstances(String clusterName) {
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    Map<String, HelixZkClient> clientMap = _liveInstanceOwners.getOrDefault(clusterName, Collections.emptyMap());

    for (String liveInstance : accessor.getChildNames(keyBuilder.liveInstances())) {
      ZKHelixDataAccessor dataAccessor =
          new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
      dataAccessor.removeProperty(keyBuilder.liveInstance(liveInstance));

      HelixZkClient client = clientMap.remove(liveInstance);
      if (client != null) {
        client.close();
      }
    }

    if (clientMap.isEmpty()) {
      _liveInstanceOwners.remove(clusterName);
    }
  }

  protected void setupInstances(String clusterName, int[] instances) {
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    for (int i = 0; i < instances.length; i++) {
      String instance = "localhost_" + instances[i];
      InstanceConfig instanceConfig = new InstanceConfig(instance);
      instanceConfig.setHostName("localhost");
      instanceConfig.setPort("" + instances[i]);
      instanceConfig.setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE);
      admin.addInstance(clusterName, instanceConfig);
    }
  }

  protected void runPipeline(ClusterEvent event, Pipeline pipeline, boolean shouldThrowException)
      throws Exception {
    try {
      pipeline.handle(event);
      pipeline.finish();
    } catch (Exception e) {
      if (shouldThrowException) {
        throw e;
      } else {
        LOG.error("Exception while executing pipeline: {}. Will not continue to next pipeline",
            pipeline, e);
      }
    }
  }

  protected void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();

    // AbstractAsyncBaseStage will run asynchronously, and it's main logics are implemented in
    // execute() function call
    // TODO (harry): duplicated code in ZkIntegrationTestBase, consider moving runStage()
    // to a shared library
    if (stage instanceof AbstractAsyncBaseStage) {
      ((AbstractAsyncBaseStage) stage).execute(event);
    } else {
      stage.process(event);
    }
    stage.postProcess();
  }

  protected void deleteCluster(String clusterName) {
    TestHelper.dropCluster(clusterName, _gZkClient, _gSetupTool);
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
  protected static class EmptyZkVerifier implements ClusterStateVerifier.ZkVerifier {
    private final String _clusterName;
    private final String _resourceName;
    private final HelixZkClient _zkClient;

    /**
     * Instantiate the verifier
     * @param clusterName the cluster to verify
     * @param resourceName the resource to verify
     */
    public EmptyZkVerifier(String clusterName, String resourceName) {
      _clusterName = clusterName;
      _resourceName = resourceName;

      _zkClient = DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR));
      _zkClient.setZkSerializer(new ZNRecordSerializer());
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

        List<String> taskSessionIds =
            accessor.getChildNames(keyBuilder.taskCurrentStateSessions(participant));
        for (String sessionId : taskSessionIds) {
          CurrentState taskCurrentState = accessor
              .getProperty(keyBuilder.taskCurrentState(participant, sessionId, _resourceName));
          Map<String, String> taskPartitionStateMap = taskCurrentState.getPartitionStateMap();
          if (taskPartitionStateMap != null && !taskPartitionStateMap.isEmpty()) {
            LOG.error("Task current state not empty for " + participant);
            return false;
          }
        }
      }
      return true;
    }

    @Override
    public ZkClient getZkClient() {
      return (ZkClient) _zkClient;
    }

    @Override
    public String getClusterName() {
      return _clusterName;
    }
  }
}
