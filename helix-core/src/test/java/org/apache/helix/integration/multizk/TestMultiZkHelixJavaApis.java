package org.apache.helix.integration.multizk;

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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.util.HttpRoutingDataReader;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * TestMultiZkHelixJavaApis spins up multiple in-memory ZooKeepers with a pre-configured
 * cluster-Zk realm routing information.
 * This test verifies that all Helix Java APIs work as expected.
 */
public class TestMultiZkHelixJavaApis {
  private static final int NUM_ZK = 3;
  private static final Map<String, ZkServer> ZK_SERVER_MAP = new HashMap<>();
  private static final Map<String, HelixZkClient> ZK_CLIENT_MAP = new HashMap<>();
  private static final Map<String, ClusterControllerManager> MOCK_CONTROLLERS = new HashMap<>();
  private static final Set<MockParticipantManager> MOCK_PARTICIPANTS = new HashSet<>();
  private static final List<String> CLUSTER_LIST =
      ImmutableList.of("CLUSTER_1", "CLUSTER_2", "CLUSTER_3");

  // For testing different MSDS endpoint configs.
  private static final String CLUSTER_ONE = CLUSTER_LIST.get(0);
  private static final String CLUSTER_FOUR = "CLUSTER_4";

  private MockMetadataStoreDirectoryServer _msds;
  private static final Map<String, Collection<String>> _rawRoutingData = new HashMap<>();
  private RealmAwareZkClient _zkClient;
  private HelixAdmin _zkHelixAdmin;

  // Save System property configs from before this test and pass onto after the test
  private Map<String, String> _configStore = new HashMap<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    // Create 3 in-memory zookeepers and routing mapping
    final String zkPrefix = "localhost:";
    final int zkStartPort = 8777;

    for (int i = 0; i < NUM_ZK; i++) {
      String zkAddress = zkPrefix + (zkStartPort + i);
      ZK_SERVER_MAP.put(zkAddress, TestHelper.startZkServer(zkAddress));
      ZK_CLIENT_MAP.put(zkAddress, DedicatedZkClientFactory.getInstance()
          .buildZkClient(new HelixZkClient.ZkConnectionConfig(zkAddress),
              new HelixZkClient.ZkClientConfig().setZkSerializer(new ZNRecordSerializer())));

      // One cluster per ZkServer created
      _rawRoutingData.put(zkAddress, Collections.singletonList("/" + CLUSTER_LIST.get(i)));
    }

    // Create a Mock MSDS
    final String msdsHostName = "localhost";
    final int msdsPort = 11117;
    final String msdsNamespace = "multiZkTest";
    _msds = new MockMetadataStoreDirectoryServer(msdsHostName, msdsPort, msdsNamespace,
        _rawRoutingData);
    _msds.startServer();

    // Save previously-set system configs
    String prevMultiZkEnabled = System.getProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
    String prevMsdsServerEndpoint =
        System.getProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
    if (prevMultiZkEnabled != null) {
      _configStore.put(SystemPropertyKeys.MULTI_ZK_ENABLED, prevMultiZkEnabled);
    }
    if (prevMsdsServerEndpoint != null) {
      _configStore
          .put(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, prevMsdsServerEndpoint);
    }

    // Turn on multiZk mode in System config
    System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED, "true");
    // MSDS endpoint: http://localhost:11117/admin/v2/namespaces/multiZkTest
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY,
        "http://" + msdsHostName + ":" + msdsPort + "/admin/v2/namespaces/" + msdsNamespace);

    // HttpRoutingDataReader's routing data may be set by other tests using the same endpoint;
    // reset() for good measure
    HttpRoutingDataReader.reset();
    // Create a FederatedZkClient for admin work
    _zkClient =
        new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig());
  }

  @AfterClass
  public void afterClass() throws Exception {
    try {
      // Kill all mock controllers and participants
      MOCK_CONTROLLERS.values().forEach(ClusterControllerManager::syncStop);
      MOCK_PARTICIPANTS.forEach(MockParticipantManager::syncStop);

      // Tear down all clusters
      CLUSTER_LIST.forEach(cluster -> TestHelper.dropCluster(cluster, _zkClient));

      // Verify that all clusters are gone in each zookeeper
      Assert.assertTrue(TestHelper.verify(() -> {
        for (Map.Entry<String, HelixZkClient> zkClientEntry : ZK_CLIENT_MAP.entrySet()) {
          List<String> children = zkClientEntry.getValue().getChildren("/");
          if (children.stream().anyMatch(CLUSTER_LIST::contains)) {
            return false;
          }
        }
        return true;
      }, TestHelper.WAIT_DURATION));

      // Tear down zookeepers
      ZK_SERVER_MAP.forEach((zkAddress, zkServer) -> zkServer.shutdown());

      // Stop MockMSDS
      _msds.stopServer();
    } finally {
      // Restore System property configs
      if (_configStore.containsKey(SystemPropertyKeys.MULTI_ZK_ENABLED)) {
        System.setProperty(SystemPropertyKeys.MULTI_ZK_ENABLED,
            _configStore.get(SystemPropertyKeys.MULTI_ZK_ENABLED));
      } else {
        System.clearProperty(SystemPropertyKeys.MULTI_ZK_ENABLED);
      }
      if (_configStore.containsKey(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY)) {
        System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY,
            _configStore.get(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY));
      } else {
        System.clearProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY);
      }
    }
  }

  /**
   * Test cluster creation according to the pre-set routing mapping.
   * Helix Java API tested is ClusterSetup in this method.
   */
  @Test
  public void testCreateClusters() {
    // Create two ClusterSetups using two different constructors
    // Note: ZK Address here could be anything because multiZk mode is on (it will be ignored)
    ClusterSetup clusterSetupZkAddr = new ClusterSetup(ZK_SERVER_MAP.keySet().iterator().next());
    ClusterSetup clusterSetupBuilder = new ClusterSetup.Builder().build();

    createClusters(clusterSetupZkAddr);
    verifyClusterCreation(clusterSetupZkAddr);

    createClusters(clusterSetupBuilder);
    verifyClusterCreation(clusterSetupBuilder);

    // Create clusters again to continue with testing
    createClusters(clusterSetupBuilder);
  }

  private void createClusters(ClusterSetup clusterSetup) {
    // Create clusters
    for (String clusterName : CLUSTER_LIST) {
      clusterSetup.addCluster(clusterName, false);
    }
  }

  private void verifyClusterCreation(ClusterSetup clusterSetup) {
    // Verify that clusters have been created correctly according to routing mapping
    _rawRoutingData.forEach((zkAddress, cluster) -> {
      // Note: clusterNamePath already contains "/"
      String clusterNamePath = cluster.iterator().next();

      // Check with single-realm ZkClients
      Assert.assertTrue(ZK_CLIENT_MAP.get(zkAddress).exists(clusterNamePath));
      // Check with realm-aware ZkClient (federated)
      Assert.assertTrue(_zkClient.exists(clusterNamePath));

      // Remove clusters
      clusterSetup
          .deleteCluster(clusterNamePath.substring(1)); // Need to remove "/" at the beginning
    });
  }

  /**
   * Test Helix Participant creation and addition.
   * Helix Java APIs tested in this method are:
   * ZkHelixAdmin and ZKHelixManager (mock participant/controller)
   */
  @Test(dependsOnMethods = "testCreateClusters")
  public void testCreateParticipants() throws Exception {
    // Create two ClusterSetups using two different constructors
    // Note: ZK Address here could be anything because multiZk mode is on (it will be ignored)
    HelixAdmin helixAdminZkAddr = new ZKHelixAdmin(ZK_SERVER_MAP.keySet().iterator().next());
    HelixAdmin helixAdminBuilder = new ZKHelixAdmin.Builder().build();
    _zkHelixAdmin = helixAdminBuilder;

    String participantNamePrefix = "Node_";
    int numParticipants = 5;
    createParticipantsAndVerify(helixAdminZkAddr, numParticipants, participantNamePrefix);
    createParticipantsAndVerify(helixAdminBuilder, numParticipants, participantNamePrefix);

    // Create mock controller and participants for next tests
    for (String cluster : CLUSTER_LIST) {
      // Start a controller
      // Note: in multiZK mode, ZK Addr is ignored
      ClusterControllerManager mockController =
          new ClusterControllerManager("DummyZK", cluster, "controller");
      mockController.syncStart();
      MOCK_CONTROLLERS.put(cluster, mockController);

      for (int i = 0; i < numParticipants; i++) {
        // Note: in multiZK mode, ZK Addr is ignored
        InstanceConfig instanceConfig = new InstanceConfig(participantNamePrefix + i);
        helixAdminBuilder.addInstance(cluster, instanceConfig);
        MockParticipantManager mockNode =
            new MockParticipantManager("DummyZK", cluster, participantNamePrefix + i);

        // Register task state model for task framework testing in later methods
        Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
        taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);
        // Register a Task state model factory.
        StateMachineEngine stateMachine = mockNode.getStateMachineEngine();
        stateMachine
            .registerStateModelFactory("Task", new TaskStateModelFactory(mockNode, taskFactoryReg));

        mockNode.syncStart();
        MOCK_PARTICIPANTS.add(mockNode);
      }
      // Check that mockNodes are up
      Assert.assertTrue(TestHelper
          .verify(() -> helixAdminBuilder.getInstancesInCluster(cluster).size() == numParticipants,
              TestHelper.WAIT_DURATION));
    }
  }

  private void createParticipantsAndVerify(HelixAdmin admin, int numParticipants,
      String participantNamePrefix) {
    // Create participants in clusters
    Set<String> participantNames = new HashSet<>();
    CLUSTER_LIST.forEach(cluster -> {
      for (int i = 0; i < numParticipants; i++) {
        String participantName = participantNamePrefix + i;
        participantNames.add(participantName);
        InstanceConfig instanceConfig = new InstanceConfig(participantNamePrefix + i);
        admin.addInstance(cluster, instanceConfig);
      }
    });

    // Verify participants have been created properly
    _rawRoutingData.forEach((zkAddress, cluster) -> {
      // Note: clusterNamePath already contains "/"
      String clusterNamePath = cluster.iterator().next();

      // Check with single-realm ZkClients
      List<String> instances =
          ZK_CLIENT_MAP.get(zkAddress).getChildren(clusterNamePath + "/INSTANCES");
      Assert.assertEquals(new HashSet<>(instances), participantNames);

      // Check with realm-aware ZkClient (federated)
      instances = _zkClient.getChildren(clusterNamePath + "/INSTANCES");
      Assert.assertEquals(new HashSet<>(instances), participantNames);

      // Remove Participants
      participantNames.forEach(participant -> {
        InstanceConfig instanceConfig = new InstanceConfig(participant);
        admin.dropInstance(clusterNamePath.substring(1), instanceConfig);
      });
    });
  }

  /**
   * Test that clusters and instances are set up properly.
   * Helix Java APIs tested in this method is ZkUtil.
   */
  @Test(dependsOnMethods = "testCreateParticipants")
  public void testZkUtil() {
    CLUSTER_LIST.forEach(cluster -> {
      _zkHelixAdmin.getInstancesInCluster(cluster).forEach(instance -> ZKUtil
          .isInstanceSetup("DummyZk", cluster, instance, InstanceType.PARTICIPANT));
    });
  }

  /**
   * Create resources and see if things get rebalanced correctly.
   * Helix Java API tested in this methods are:
   * ZkBaseDataAccessor
   * ZkHelixClusterVerifier (BestPossible)
   */
  @Test(dependsOnMethods = "testZkUtil")
  public void testCreateAndRebalanceResources() {
    BaseDataAccessor<ZNRecord> dataAccessorZkAddr = new ZkBaseDataAccessor<>("DummyZk");
    BaseDataAccessor<ZNRecord> dataAccessorBuilder =
        new ZkBaseDataAccessor.Builder<ZNRecord>().build();

    String resourceNamePrefix = "DB_";
    int numResources = 5;
    int numPartitions = 3;
    Map<String, Map<String, ZNRecord>> idealStateMap = new HashMap<>();

    for (String cluster : CLUSTER_LIST) {
      Set<String> resourceNames = new HashSet<>();
      Set<String> liveInstancesNames = new HashSet<>(dataAccessorZkAddr
          .getChildNames("/" + cluster + "/LIVEINSTANCES", AccessOption.PERSISTENT));

      for (int i = 0; i < numResources; i++) {
        String resource = cluster + "_" + resourceNamePrefix + i;
        _zkHelixAdmin.addResource(cluster, resource, numPartitions, "MasterSlave",
            IdealState.RebalanceMode.FULL_AUTO.name());
        _zkHelixAdmin.rebalance(cluster, resource, 3);
        resourceNames.add(resource);

        // Update IdealState fields with ZkBaseDataAccessor
        String resourcePath = "/" + cluster + "/IDEALSTATES/" + resource;
        ZNRecord is = dataAccessorZkAddr.get(resourcePath, null, AccessOption.PERSISTENT);
        is.setSimpleField(RebalanceConfig.RebalanceConfigProperty.REBALANCER_CLASS_NAME.name(),
            DelayedAutoRebalancer.class.getName());
        is.setSimpleField(RebalanceConfig.RebalanceConfigProperty.REBALANCE_STRATEGY.name(),
            CrushEdRebalanceStrategy.class.getName());
        dataAccessorZkAddr.set(resourcePath, is, AccessOption.PERSISTENT);
        idealStateMap.computeIfAbsent(cluster, recordList -> new HashMap<>())
            .putIfAbsent(is.getId(), is); // Save ZNRecord for comparison later
      }

      // Create a verifier to make sure all resources have been rebalanced
      ZkHelixClusterVerifier verifier =
          new BestPossibleExternalViewVerifier.Builder(cluster).setResources(resourceNames)
              .setExpectLiveInstances(liveInstancesNames).build();
      Assert.assertTrue(verifier.verifyByPolling());
    }

    // Using the ZkBaseDataAccessor created using the Builder, check that the correct IS is read
    for (String cluster : CLUSTER_LIST) {
      Map<String, ZNRecord> savedIdealStates = idealStateMap.get(cluster);
      List<String> resources = dataAccessorBuilder
          .getChildNames("/" + cluster + "/IDEALSTATES", AccessOption.PERSISTENT);
      resources.forEach(resource -> {
        ZNRecord is = dataAccessorBuilder
            .get("/" + cluster + "/IDEALSTATES/" + resource, null, AccessOption.PERSISTENT);
        Assert
            .assertEquals(is.getSimpleFields(), savedIdealStates.get(is.getId()).getSimpleFields());
      });
    }
  }

  /**
   * This method tests ConfigAccessor.
   */
  @Test(dependsOnMethods = "testCreateAndRebalanceResources")
  public void testConfigAccessor() {
    // Build two ConfigAccessors to read and write:
    // 1. ConfigAccessor using a deprecated constructor
    // 2. ConfigAccessor using the Builder
    ConfigAccessor configAccessorZkAddr = new ConfigAccessor("DummyZk");
    ConfigAccessor configAccessorBuilder = new ConfigAccessor.Builder().build();

    setClusterConfigAndVerify(configAccessorZkAddr);
    setClusterConfigAndVerify(configAccessorBuilder);
  }

  private void setClusterConfigAndVerify(ConfigAccessor configAccessorMultiZk) {
    _rawRoutingData.forEach((zkAddr, clusterNamePathList) -> {
      // Need to rid of "/" because this is a sharding key
      String cluster = clusterNamePathList.iterator().next().substring(1);
      ClusterConfig clusterConfig = new ClusterConfig(cluster);
      clusterConfig.getRecord().setSimpleField("configAccessor", cluster);
      configAccessorMultiZk.setClusterConfig(cluster, clusterConfig);

      // Now check with a single-realm ConfigAccessor
      ConfigAccessor configAccessorSingleZk =
          new ConfigAccessor.Builder().setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
              .setZkAddress(zkAddr).build();
      Assert.assertEquals(configAccessorSingleZk.getClusterConfig(cluster), clusterConfig);

      // Also check with a single-realm dedicated ZkClient
      ZNRecord clusterConfigRecord =
          ZK_CLIENT_MAP.get(zkAddr).readData("/" + cluster + "/CONFIGS/CLUSTER/" + cluster);
      Assert.assertEquals(clusterConfigRecord, clusterConfig.getRecord());

      // Clean up
      clusterConfig = new ClusterConfig(cluster);
      configAccessorMultiZk.setClusterConfig(cluster, clusterConfig);
    });
  }

  /**
   * This test submits multiple tasks to be run.
   * The Helix Java APIs tested in this method are TaskDriver (HelixManager) and
   * ZkHelixPropertyStore/ZkCacheBaseDataAccessor.
   */
  @Test(dependsOnMethods = "testConfigAccessor")
  public void testTaskFramework() throws InterruptedException {
    // Note: TaskDriver is like HelixManager - it only operates on one designated
    // Create TaskDrivers for all clusters
    Map<String, TaskDriver> taskDriverMap = new HashMap<>();
    MOCK_CONTROLLERS
        .forEach((cluster, controller) -> taskDriverMap.put(cluster, new TaskDriver(controller)));

    // Create a Task Framework workload and start
    Workflow workflow = WorkflowGenerator.generateNonTargetedSingleWorkflowBuilder("job").build();
    for (TaskDriver taskDriver : taskDriverMap.values()) {
      taskDriver.start(workflow);
    }

    // Use multi-ZK ZkHelixPropertyStore/ZkCacheBaseDataAccessor to query for workflow/job states
    HelixPropertyStore<ZNRecord> propertyStore =
        new ZkHelixPropertyStore.Builder<ZNRecord>().build();
    for (Map.Entry<String, TaskDriver> entry : taskDriverMap.entrySet()) {
      String cluster = entry.getKey();
      TaskDriver driver = entry.getValue();
      // Wait until workflow has completed
      TaskState wfStateFromTaskDriver =
          driver.pollForWorkflowState(workflow.getName(), TaskState.COMPLETED);
      String workflowContextPath =
          "/" + cluster + "/PROPERTYSTORE/TaskRebalancer/" + workflow.getName() + "/Context";
      ZNRecord workflowContextRecord =
          propertyStore.get(workflowContextPath, null, AccessOption.PERSISTENT);
      WorkflowContext context = new WorkflowContext(workflowContextRecord);

      // Compare the workflow state read from PropertyStore and TaskDriver
      Assert.assertEquals(context.getWorkflowState(), wfStateFromTaskDriver);
    }
  }

  /**
   * This method tests that ZKHelixAdmin::getClusters() works in multi-zk environment.
   */
  @Test(dependsOnMethods = "testTaskFramework")
  public void testGetAllClusters() {
    Assert.assertEquals(new HashSet<>(_zkHelixAdmin.getClusters()), new HashSet<>(CLUSTER_LIST));
  }

  /**
   * Tests Helix Java APIs which use different MSDS endpoint configs. Java API should
   * only connect to the configured MSDS but not the others. The APIs are explicitly tested are:
   * - ClusterSetup
   * - HelixAdmin
   * - ZkUtil
   * - HelixManager
   * - BaseDataAccessor
   * - ConfigAccessor
   */
  @Test(dependsOnMethods = "testGetAllClusters")
  public void testDifferentMsdsEndpointConfigs() throws IOException, InvalidRoutingDataException {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("Start " + methodName);
    final String zkAddress = ZK_SERVER_MAP.keySet().iterator().next();
    final Map<String, Collection<String>> secondRoutingData =
        ImmutableMap.of(zkAddress, Collections.singletonList(formPath(CLUSTER_FOUR)));
    MockMetadataStoreDirectoryServer secondMsds =
        new MockMetadataStoreDirectoryServer("localhost", 11118, "multiZkTest", secondRoutingData);
    final RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setMsdsEndpoint(secondMsds.getEndpoint()).build();
    secondMsds.startServer();

    try {
      // Verify ClusterSetup
      verifyClusterSetupMsdsEndpoint(connectionConfig);

      // Verify HelixAdmin
      verifyHelixAdminMsdsEndpoint(connectionConfig);

      // Verify ZKUtil
      verifyZkUtilMsdsEndpoint();

      // Verify HelixManager
      verifyHelixManagerMsdsEndpoint();

      // Verify BaseDataAccessor
      verifyBaseDataAccessorMsdsEndpoint(connectionConfig);

      // Verify ConfigAccessor
      verifyConfigAccessorMsdsEndpoint(connectionConfig);
    } finally {
      RealmAwareZkClient zkClient = new FederatedZkClient(connectionConfig,
          new RealmAwareZkClient.RealmAwareZkClientConfig());
      TestHelper.dropCluster(CLUSTER_FOUR, zkClient);
      zkClient.close();
      secondMsds.stopServer();
    }
    System.out.println("End " + methodName);
  }

  private void verifyHelixManagerMsdsEndpoint() {
    System.out.println("Start " + TestHelper.getTestMethodName());

    // Mock participants are already created and started in the previous test.
    // The mock participant only connects to MSDS configured in system property,
    // but not the other.
    final MockParticipantManager manager = MOCK_PARTICIPANTS.iterator().next();
    verifyMsdsZkRealm(CLUSTER_ONE, true,
        () -> manager.getZkClient().exists(formPath(manager.getClusterName())));
    verifyMsdsZkRealm(CLUSTER_FOUR, false,
        () -> manager.getZkClient().exists(formPath(CLUSTER_FOUR)));
  }

  private void verifyBaseDataAccessorMsdsEndpoint(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig) {
    System.out.println("Start " + TestHelper.getTestMethodName());
    // MSDS endpoint is not configured in builder, so config in system property is used.
    BaseDataAccessor<ZNRecord> firstDataAccessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>().build();

    // Create base data accessor with MSDS endpoint configured in builder.
    BaseDataAccessor<ZNRecord> secondDataAccessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>().setRealmAwareZkConnectionConfig(connectionConfig)
            .build();

    String methodName = TestHelper.getTestMethodName();
    String clusterOnePath = formPath(CLUSTER_ONE, methodName);
    String clusterFourPath = formPath(CLUSTER_FOUR, methodName);
    ZNRecord record = new ZNRecord(methodName);

    try {
      firstDataAccessor.create(clusterOnePath, record, AccessOption.PERSISTENT);
      secondDataAccessor.create(clusterFourPath, record, AccessOption.PERSISTENT);

      // Verify data accessors that they could only talk to their own configured MSDS endpoint:
      // either being set in builder or system property.
      Assert.assertTrue(firstDataAccessor.exists(clusterOnePath, AccessOption.PERSISTENT));
      verifyMsdsZkRealm(CLUSTER_FOUR, false,
          () -> firstDataAccessor.exists(clusterFourPath, AccessOption.PERSISTENT));

      Assert.assertTrue(secondDataAccessor.exists(clusterFourPath, AccessOption.PERSISTENT));
      verifyMsdsZkRealm(CLUSTER_ONE, false,
          () -> secondDataAccessor.exists(clusterOnePath, AccessOption.PERSISTENT));

      firstDataAccessor.remove(clusterOnePath, AccessOption.PERSISTENT);
      secondDataAccessor.remove(clusterFourPath, AccessOption.PERSISTENT);

      Assert.assertFalse(firstDataAccessor.exists(clusterOnePath, AccessOption.PERSISTENT));
      Assert.assertFalse(secondDataAccessor.exists(clusterFourPath, AccessOption.PERSISTENT));
    } finally {
      firstDataAccessor.close();
      secondDataAccessor.close();
    }
  }

  private void verifyClusterSetupMsdsEndpoint(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig) {
    System.out.println("Start " + TestHelper.getTestMethodName());

    ClusterSetup firstClusterSetup = new ClusterSetup.Builder().build();
    ClusterSetup secondClusterSetup =
        new ClusterSetup.Builder().setRealmAwareZkConnectionConfig(connectionConfig).build();

    try {
      verifyMsdsZkRealm(CLUSTER_ONE, true,
          () -> firstClusterSetup.addCluster(CLUSTER_ONE, false));
      verifyMsdsZkRealm(CLUSTER_FOUR, false,
          () -> firstClusterSetup.addCluster(CLUSTER_FOUR, false));

      verifyMsdsZkRealm(CLUSTER_FOUR, true,
          () -> secondClusterSetup.addCluster(CLUSTER_FOUR, false));
      verifyMsdsZkRealm(CLUSTER_ONE, false,
          () -> secondClusterSetup.addCluster(CLUSTER_ONE, false));
    } finally {
      firstClusterSetup.close();
      secondClusterSetup.close();
    }
  }

  private void verifyZkUtilMsdsEndpoint() {
    System.out.println("Start " + TestHelper.getTestMethodName());
    String dummyZkAddress = "dummyZkAddress";

    // MSDS endpoint 1
    verifyMsdsZkRealm(CLUSTER_ONE, true,
        () -> ZKUtil.getChildren(dummyZkAddress, formPath(CLUSTER_ONE)));
    // Verify MSDS endpoint 2 is not used by this ZKUtil.
    verifyMsdsZkRealm(CLUSTER_FOUR, false,
        () -> ZKUtil.getChildren(dummyZkAddress, formPath(CLUSTER_FOUR)));
  }

  private void verifyHelixAdminMsdsEndpoint(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig) {
    System.out.println("Start " + TestHelper.getTestMethodName());

    HelixAdmin firstHelixAdmin = new ZKHelixAdmin.Builder().build();
    HelixAdmin secondHelixAdmin =
        new ZKHelixAdmin.Builder().setRealmAwareZkConnectionConfig(connectionConfig).build();

    try {
      verifyMsdsZkRealm(CLUSTER_ONE, true,
          () -> firstHelixAdmin.enableCluster(CLUSTER_ONE, true));
      verifyMsdsZkRealm(CLUSTER_FOUR, false,
          () -> firstHelixAdmin.enableCluster(CLUSTER_FOUR, true));

      verifyMsdsZkRealm(CLUSTER_FOUR, true,
          () -> secondHelixAdmin.enableCluster(CLUSTER_FOUR, true));
      verifyMsdsZkRealm(CLUSTER_ONE, false,
          () -> secondHelixAdmin.enableCluster(CLUSTER_ONE, true));
    } finally {
      firstHelixAdmin.close();
      secondHelixAdmin.close();
    }
  }

  private void verifyConfigAccessorMsdsEndpoint(
      RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig) {
    System.out.println("Start " + TestHelper.getTestMethodName());

    ConfigAccessor firstConfigAccessor = new ConfigAccessor.Builder().build();
    ConfigAccessor secondConfigAccessor =
        new ConfigAccessor.Builder().setRealmAwareZkConnectionConfig(connectionConfig).build();

    try {
      verifyMsdsZkRealm(CLUSTER_ONE, true,
          () -> firstConfigAccessor.getClusterConfig(CLUSTER_ONE));
      verifyMsdsZkRealm(CLUSTER_FOUR, false,
          () -> firstConfigAccessor.getClusterConfig(CLUSTER_FOUR));

      verifyMsdsZkRealm(CLUSTER_FOUR, true,
          () -> secondConfigAccessor.getClusterConfig(CLUSTER_FOUR));
      verifyMsdsZkRealm(CLUSTER_ONE, false,
          () -> secondConfigAccessor.getClusterConfig(CLUSTER_ONE));
    } finally {
      firstConfigAccessor.close();
      secondConfigAccessor.close();
    }
  }

  private interface Operation {
    void run();
  }

  private void verifyMsdsZkRealm(String cluster, boolean shouldSucceed, Operation operation) {
    try {
      operation.run();
      if (!shouldSucceed) {
        Assert.fail("Should not connect to the MSDS that has /" + cluster);
      }
    } catch (NoSuchElementException e) {
      if (shouldSucceed) {
        Assert.fail("ZK Realm should be found for /" + cluster);
      } else {
        Assert.assertTrue(
            e.getMessage().startsWith("Cannot find ZK realm for the path: /" + cluster));
      }
    } catch (IllegalArgumentException e) {
      if (shouldSucceed) {
        Assert.fail(formPath(cluster) + " should be a valid sharding key.");
      } else {
        String messageOne = "Given path: /" + cluster + " does not have a "
            + "valid sharding key or its ZK sharding key is not found in the cached routing data";
        String messageTwo = "Given path: /" + cluster + "'s ZK sharding key: /" + cluster
            + " does not match the ZK sharding key";
        Assert.assertTrue(
            e.getMessage().startsWith(messageOne) || e.getMessage().startsWith(messageTwo));
      }
    } catch (HelixException e) {
      // NoSuchElementException: "ZK Realm not found!" is swallowed in ZKUtil.isClusterSetup()
      // Instead, HelixException is thrown.
      if (shouldSucceed) {
        Assert.fail("Cluster: " + cluster + " should have been setup.");
      } else {
        Assert.assertEquals("fail to get config. cluster: " + cluster + " is NOT setup.",
            e.getMessage());
      }
    }
  }

  private String formPath(String... pathNames) {
    StringBuilder sb = new StringBuilder();
    for (String name : pathNames) {
      sb.append('/').append(name);
    }
    return sb.toString();
  }
}
