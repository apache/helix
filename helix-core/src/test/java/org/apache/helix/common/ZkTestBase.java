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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.common.execution.TestExecutionFlow;
import org.apache.helix.common.execution.TestExecutionRuntime;
import org.apache.helix.controller.pipeline.AbstractAsyncBaseStage;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;


public class ZkTestBase extends TestExecutionRuntime {
  private static final Logger LOG = LoggerFactory.getLogger(ZkTestBase.class);

  public static final String ZK_ADDR = ZK_PREFIX + ZK_START_PORT;
  protected static final String CLUSTER_PREFIX = "CLUSTER";
  protected static final String CONTROLLER_CLUSTER_PREFIX = "CONTROLLER_CLUSTER";
  protected static final String CONTROLLER_PREFIX = "controller";
  protected static final String PARTICIPANT_PREFIX = "localhost";

  @BeforeSuite
  public void beforeSuite() {
    java.util.logging.Logger topJavaLogger = java.util.logging.Logger.getLogger("");
    topJavaLogger.setLevel(Level.WARNING);

    // Due to ZOOKEEPER-2693 fix, we need to specify whitelist for execute zk commends
    System.setProperty("zookeeper.4lw.commands.whitelist", "*");
    System.setProperty(SystemPropertyKeys.CONTROLLER_MESSAGE_PURGE_DELAY, "3000");
    initialize();
  }

  @AfterSuite
  public void afterSuite() throws IOException {
    unwind();
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    cleanupJMXObjects();
    // Giving each test some time to settle (such as gc pause, etc).
    // Note that this is the best effort we could make to stabilize tests, not a complete solution
    Runtime.getRuntime().gc();
    Thread.sleep(MANUAL_GC_PAUSE);
  }

  @AfterClass
  public void cleanupLiveInstanceOwners(ITestContext testContext) {
    System.out.println("AfterClass: " + testContext.getName() + " called.");
    for (String cluster : getLiveInstanceOwners().keySet()) {
      Map<String, HelixZkClient> clientMap = getLiveInstanceOwners().get(cluster);
      for (HelixZkClient client : clientMap.values()) {
        client.close();
      }
      clientMap.clear();
    }
    getLiveInstanceOwners().clear();
  }

  @BeforeMethod
  public void beforeTest(Method testMethod, ITestContext testContext) {
    Long startTime = System.currentTimeMillis();
    testContext.setAttribute("StartTime", startTime);
    System.out.println(String.format("Test-case: %s started at: %s", testMethod.getName(), new Date(startTime)));
  }

  @AfterMethod
  public void afterTest(Method testMethod, ITestContext testContext) {
    Long startTime = (Long) testContext.getAttribute("StartTime");
    Long endTime = System.currentTimeMillis();
    System.out.println(String.format("Test-case: %s ended at: %s, took total time: %s ms",
        testMethod.getName(), new Date(endTime), endTime - startTime));
  }

  public TestExecutionFlow createClusterTestExecutionFlow() {
    return new TestExecutionFlow(_gZkClient);
  }

  protected String getShortClassName() {
    return this.getClass().getSimpleName();
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

  private IdealState createResource(String clusterName, String db, String stateModel,
      int numPartition, int replica, int minActiveReplica, long delay, String rebalancerClassName,
      String rebalanceStrategy) {
    IdealState idealState = _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, db);

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

    IdealState is = _gSetupTool.getClusterManagementTool().getResourceIdealState(clusterName, dbName);
    for (String p : is.getPartitionSet()) {
      is.setPreferenceList(p, preferenceList);
    }
    clusterSetup.getClusterManagementTool().setResourceIdealState(clusterName, dbName, is);
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
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<>(_gZkClient));
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

  protected List<LiveInstance> setupLiveInstances(String clusterName, int[] liveInstances) {
    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new ZNRecordSerializer());

    List<LiveInstance> result = new ArrayList<>();
    Map<String, Map<String, HelixZkClient>> liveInstanceOwners = getLiveInstanceOwners();

    for (int i = 0; i < liveInstances.length; i++) {
      String instance = "localhost_" + liveInstances[i];

      liveInstanceOwners.putIfAbsent(clusterName, new HashMap<>());
      Map<String, HelixZkClient> clientMap = liveInstanceOwners.get(clusterName);
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

    Map<String, Map<String, HelixZkClient>> liveInstanceOwners = getLiveInstanceOwners();
    Map<String, HelixZkClient> clientMap = liveInstanceOwners.getOrDefault(clusterName, Collections.emptyMap());

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
      liveInstanceOwners.remove(clusterName);
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
   * @param clazz the HelixProperty subclass
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

}
