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
import java.lang.IllegalStateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.exception.InvalidRoutingDataException;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.api.client.ZkClientType;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.exception.MultiZkException;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Some Helix user do not have multizk routing configs in jvm system config but pass
 * in through RealAwareZkConnectionConfig.
 * This test class will not set jvm routing zk config and test Helix functionality.
 * Tests were similar to TestMultiZkHelixJavaApis but without "MSDS_SERVER_ENDPOINT_KEY"
 * in system property
 */
public class TestMultiZkConnectionConfig extends MultiZkTestBase {
  protected ClusterSetup _clusterSetupZkAddr;
  protected ClusterSetup _clusterSetupBuilder;
  protected RealmAwareZkClient.RealmAwareZkConnectionConfig _invalidZkConnectionConfig;
  protected RealmAwareZkClient.RealmAwareZkConnectionConfig _validZkConnectionConfig;

  // For testing different MSDS endpoint configs.
  private static final String CLUSTER_ONE = CLUSTER_LIST.get(0);
  private static final String CLUSTER_FOUR = "CLUSTER_4";
  private static String _className = TestHelper.getTestClassName();

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    // Routing data may be set by other tests using the same endpoint; reset() for good measure
    RoutingDataManager.getInstance().reset(true);
    // Create a FederatedZkClient for admin work
    _zkClient =
        new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig());
    _zkClient.setZkSerializer(new ZNRecordSerializer());
    System.out.println("end start");
  }

  public void beforeApiClass() throws Exception {
    beforeClass();
    // MSDS endpoint: http://localhost:11117/admin/v2/namespaces/multiZkTest
    System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, _msdsEndpoint);

    // Routing data may be set by other tests using the same endpoint; reset() for good measure
    RoutingDataManager.getInstance().reset(true);
    // Create a FederatedZkClient for admin work
    _zkClient =
        new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder().build(),
            new RealmAwareZkClient.RealmAwareZkClientConfig());
    System.out.println("end start");
  }

  /**
   * Test cluster creation according to the pre-set routing mapping.
   * Helix Java API tested is ClusterSetup in this method.
   */
  @Test
  public void testCreateClusters() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    setupCluster();

    createClusters(_clusterSetupZkAddr);
    verifyClusterCreation(_clusterSetupZkAddr);

    createClusters(_clusterSetupBuilder);
    verifyClusterCreation(_clusterSetupBuilder);

    // Create clusters again to continue with testing
    createClusters(_clusterSetupBuilder);

    _clusterSetupZkAddr.close();
    _clusterSetupBuilder.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  public void setupCluster() {
    // Create two ClusterSetups using two different constructors
    // Note: ZK Address here could be anything because multiZk mode is on (it will be ignored)
    _clusterSetupZkAddr = new ClusterSetup(_zkClient);
    _clusterSetupBuilder = new ClusterSetup.Builder().setRealmAwareZkConnectionConfig(
                    new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
                            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
                            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build())
            .build();
  }

  public void createClusters(ClusterSetup clusterSetup) {
    // Create clusters
    for (String clusterName : CLUSTER_LIST) {
      clusterSetup.addCluster(clusterName, false);
    }
  }

  public void verifyClusterCreation(ClusterSetup clusterSetup) {
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
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    // Create two ClusterSetups using two different constructors
    // Note: ZK Address here could be anything because multiZk mode is on (it will be ignored)
    RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build();
    HelixAdmin helixAdminBuilder =
        new ZKHelixAdmin.Builder().setRealmAwareZkConnectionConfig(zkConnectionConfig).build();
    _zkHelixAdmin =
        new ZKHelixAdmin.Builder().setRealmAwareZkConnectionConfig(zkConnectionConfig).build();

    String participantNamePrefix = "Node_";
    int numParticipants = 5;
    createParticipantsAndVerify(helixAdminBuilder, numParticipants, participantNamePrefix);

    // Create mock controller and participants for next tests
    for (String cluster : CLUSTER_LIST) {
      // Start a controller
      // Note: in multiZK mode, ZK Addr is ignored
      RealmAwareZkClient.RealmAwareZkConnectionConfig zkConnectionConfigCls =
          new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
              .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
              .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name())
              .setZkRealmShardingKey("/" + cluster).build();
      HelixManagerProperty.Builder propertyBuilder = new HelixManagerProperty.Builder();
      HelixManagerProperty helixManagerProperty =
          propertyBuilder.setRealmAWareZkConnectionConfig(zkConnectionConfigCls).build();
      ClusterControllerManager mockController =
          new ClusterControllerManager(cluster, helixManagerProperty);
      mockController.syncStart();
      MOCK_CONTROLLERS.put(cluster, mockController);

      for (int i = 0; i < numParticipants; i++) {
        // Note: in multiZK mode, ZK Addr is ignored
        InstanceConfig instanceConfig = new InstanceConfig(participantNamePrefix + i);
        helixAdminBuilder.addInstance(cluster, instanceConfig);
        MockParticipantManager mockNode =
            new MockParticipantManager(cluster, participantNamePrefix + i, helixManagerProperty, 10,
                null);

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

    helixAdminBuilder.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  protected void createParticipantsAndVerify(HelixAdmin admin, int numParticipants,
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
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Test(dependsOnMethods = "testCreateParticipants")
  public void testZKHelixManager() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_1";
    String participantName = "HelixManager";
    InstanceConfig instanceConfig = new InstanceConfig(participantName);
    _zkHelixAdmin.addInstance(clusterName, instanceConfig);

    createZkConnectionConfigs(clusterName);

    HelixManagerProperty.Builder propertyBuilder = new HelixManagerProperty.Builder();
    try {
      HelixManager invalidManager = HelixManagerFactory
          .getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, null,
              propertyBuilder.setRealmAWareZkConnectionConfig(_invalidZkConnectionConfig).build());
      Assert.fail("Should see a HelixException here because the connection config doesn't have the "
          + "sharding key set!");
    } catch (HelixException e) {
      // Expected
    }

    // Connect as a participant
    HelixManager managerParticipant = HelixManagerFactory
        .getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, null,
            propertyBuilder.setRealmAWareZkConnectionConfig(_validZkConnectionConfig).build());
    managerParticipant.connect();

    // Connect as an administrator
    HelixManager managerAdministrator = HelixManagerFactory
        .getZKHelixManager(clusterName, participantName, InstanceType.ADMINISTRATOR, null,
            propertyBuilder.setRealmAWareZkConnectionConfig(_validZkConnectionConfig).build());
    managerAdministrator.connect();

    // Perform assert checks to make sure the manager can read and register itself as a participant
    InstanceConfig instanceConfigRead = managerAdministrator.getClusterManagmentTool()
        .getInstanceConfig(clusterName, participantName);
    Assert.assertNotNull(instanceConfigRead);
    Assert.assertEquals(instanceConfig.getInstanceName(), participantName);
    Assert.assertNotNull(managerAdministrator.getHelixDataAccessor().getProperty(
        managerAdministrator.getHelixDataAccessor().keyBuilder().liveInstance(participantName)));

    // Clean up
    managerParticipant.disconnect();
    managerAdministrator.disconnect();
    _zkHelixAdmin.dropInstance(clusterName, instanceConfig);

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  protected void createZkConnectionConfigs(String clusterName) {
    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
            new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    // Try with a connection config without ZK realm sharding key set (should fail)
    _invalidZkConnectionConfig =
            connectionConfigBuilder.build();
    _validZkConnectionConfig =
            connectionConfigBuilder
                    .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
                    .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name())
                    .setZkRealmShardingKey("/" + clusterName).build();
  }

  /**
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Test(dependsOnMethods = "testZKHelixManager")
  public void testZKHelixManagerCloudConfig() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    String clusterName = "CLUSTER_1";
    String participantName = "HelixManager";
    InstanceConfig instanceConfig = new InstanceConfig(participantName);
    _zkHelixAdmin.addInstance(clusterName, instanceConfig);

    HelixManagerProperty.Builder propertyBuilder = new HelixManagerProperty.Builder();

    // create a dummy cloud config and pass to ManagerFactory. It should be overwritten by
    // a default config because there is no CloudConfig ZNode in ZK.
    CloudConfig.Builder cloudConfigBuilder = new CloudConfig.Builder();
    cloudConfigBuilder.setCloudEnabled(true);
    // Set to Customized so CloudInfoSources and CloudInfoProcessorName will be read from cloud config
    // instead of properties
    cloudConfigBuilder.setCloudProvider(CloudProvider.CUSTOMIZED);
    cloudConfigBuilder.setCloudID("TestID");
    List<String> infoURL = new ArrayList<String>();
    infoURL.add("TestURL");
    cloudConfigBuilder.setCloudInfoSources(infoURL);
    cloudConfigBuilder.setCloudInfoProcessorName("TestProcessor");

    CloudConfig cloudConfig = cloudConfigBuilder.build();
    HelixCloudProperty oldCloudProperty = new HelixCloudProperty(cloudConfig);
    HelixManagerProperty helixManagerProperty =
        propertyBuilder.setRealmAWareZkConnectionConfig(_validZkConnectionConfig)
            .setHelixCloudProperty(oldCloudProperty).build();
    // Cloud property populated with fields defined in cloud config
    oldCloudProperty.populateFieldsWithCloudConfig(cloudConfig);
    // Add some property fields to cloud property that are not in cloud config
    Properties properties = new Properties();
    oldCloudProperty.setCustomizedCloudProperties(properties);

    class TestZKHelixManager extends ZKHelixManager {
      public TestZKHelixManager(String clusterName, String participantName,
          InstanceType instanceType, String zkAddress, HelixManagerStateListener stateListener,
          HelixManagerProperty helixManagerProperty) {
        super(clusterName, participantName, instanceType, zkAddress, stateListener,
            helixManagerProperty);
      }

      public HelixManagerProperty getHelixManagerProperty() {
        return _helixManagerProperty;
      }
    }
    // Connect as a participant
    TestZKHelixManager managerParticipant =
        new TestZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, null, null,
            helixManagerProperty);
    managerParticipant.connect();
    HelixCloudProperty newCloudProperty =
        managerParticipant.getHelixManagerProperty().getHelixCloudProperty();

    // Test reading from zk cloud config overwrite property fields included in cloud config
    Assert.assertFalse(newCloudProperty.getCloudEnabled());
    Assert.assertNull(newCloudProperty.getCloudId());
    Assert.assertNull(newCloudProperty.getCloudProvider());

    // Test non-cloud config fields are not overwritten after reading cloud config from zk
    Assert.assertEquals(newCloudProperty.getCustomizedCloudProperties(), properties);
    Assert.assertEquals(newCloudProperty.getCloudInfoSources(), infoURL);
    Assert.assertEquals(newCloudProperty.getCloudInfoProcessorName(), "TestProcessor");

    // Clean up
    managerParticipant.disconnect();
    _zkHelixAdmin.dropInstance(clusterName, instanceConfig);

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  private void setupApiCluster() {
    // Create two ClusterSetups using two different constructors
    // Note: ZK Address here could be anything because multiZk mode is on (it will be ignored)
    _clusterSetupZkAddr = new ClusterSetup(ZK_SERVER_MAP.keySet().iterator().next());
    _clusterSetupBuilder = new ClusterSetup.Builder().build();
  }

  private void createApiZkConnectionConfigs(String clusterName) {
    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    // Try with a connection config without ZK realm sharding key set (should fail)
    _invalidZkConnectionConfig = connectionConfigBuilder.build();
    _validZkConnectionConfig = connectionConfigBuilder.setZkRealmShardingKey("/" + clusterName).build();
  }

  @Test (dependsOnMethods = "testZKHelixManagerCloudConfig")
  public void testApiCreateClusters() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
    super.afterClass();

    beforeApiClass();
    setupApiCluster();

    createClusters(_clusterSetupZkAddr);
    verifyClusterCreation(_clusterSetupZkAddr);

    createClusters(_clusterSetupBuilder);
    verifyClusterCreation(_clusterSetupBuilder);

    // Create clusters again to continue with testing
    createClusters(_clusterSetupBuilder);

    _clusterSetupZkAddr.close();
    _clusterSetupBuilder.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testApiCreateClusters")
  public void testApiCreateParticipants() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
    testCreateParticipants();
    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testApiCreateParticipants")
  public void testApiZKHelixManager() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
    testZKHelixManager();
    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testApiZKHelixManager")
  public void testApiZKHelixManagerCloudConfig() throws Exception {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
    //todo
    testZKHelixManagerCloudConfig();
    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testApiZKHelixManager")
  public void testZkUtil() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    CLUSTER_LIST.forEach(cluster -> {
      _zkHelixAdmin.getInstancesInCluster(cluster).forEach(instance -> ZKUtil
          .isInstanceSetup("DummyZk", cluster, instance, InstanceType.PARTICIPANT));
    });

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * Create resources and see if things get rebalanced correctly.
   * Helix Java API tested in this methods are:
   * ZkBaseDataAccessor
   * ZkHelixClusterVerifier (BestPossible)
   */
  @Test(dependsOnMethods = "testZkUtil")
  public void testCreateAndRebalanceResources() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

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
            IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
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
              .setExpectLiveInstances(liveInstancesNames)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
      try {
        Assert.assertTrue(verifier.verifyByPolling());
      } finally {
        verifier.close();
      }
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

    dataAccessorZkAddr.close();
    dataAccessorBuilder.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * This method tests ConfigAccessor.
   */
  @Test(dependsOnMethods = "testCreateAndRebalanceResources")
  public void testConfigAccessor() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    // Build two ConfigAccessors to read and write:
    // 1. ConfigAccessor using a deprecated constructor
    // 2. ConfigAccessor using the Builder
    ConfigAccessor configAccessorZkAddr = new ConfigAccessor("DummyZk");
    ConfigAccessor configAccessorBuilder = new ConfigAccessor.Builder().build();

    setClusterConfigAndVerify(configAccessorZkAddr);
    setClusterConfigAndVerify(configAccessorBuilder);

    configAccessorZkAddr.close();
    configAccessorBuilder.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
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
      configAccessorSingleZk.close();
    });
  }

  /**
   * This test submits multiple tasks to be run.
   * The Helix Java APIs tested in this method are TaskDriver (HelixManager) and
   * ZkHelixPropertyStore/ZkCacheBaseDataAccessor.
   */
  @Test(dependsOnMethods = "testConfigAccessor")
  public void testTaskFramework() throws InterruptedException {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

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

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * This method tests that ZKHelixAdmin::getClusters() works in multi-zk environment.
   */
  @Test(dependsOnMethods = "testTaskFramework")
  public void testGetAllClusters() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    Assert.assertEquals(new HashSet<>(_zkHelixAdmin.getClusters()), new HashSet<>(CLUSTER_LIST));

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  /**
   * This method tests that GenericBaseDataAccessorBuilder and GenericZkHelixApiBuilder work as
   * expected. This test focuses on various usage scenarios for ZkBaseDataAccessor.
   *
   * Use cases tested:
   * - Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address set
   * - Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address not set, ZK sharding key set
   * - Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address set, ZK sharding key set (ZK addr should override)
   * - Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address set
   * - Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address not set, ZK sharding key set
   * - Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address set, ZK sharding key set (ZK addr should override)
   * - Create ZkBaseDataAccessor, single-realm, federated ZkClient (should fail)
   * - Create ZkBaseDataAccessor, multi-realm, dedicated ZkClient (should fail)
   * - Create ZkBaseDataAccessor, multi-realm, shared ZkClient (should fail)
   * - Create ZkBaseDataAccessor, multi-realm, federated ZkClient, ZkAddress set (should fail)
   * - Create ZkBaseDataAccessor, multi-realm, federated ZkClient, Zk sharding key set (should fail because by definition, multi-realm can access multiple sharding keys)
   * - Create ZkBaseDataAccessor, multi-realm, federated ZkClient
   * - Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, No ZkAddress set, ConnectionConfig has an invalid ZK sharding key (should fail because it cannot find a valid ZK to connect to)
   */
  @Test(dependsOnMethods = "testGetAllClusters")
  public void testGenericBaseDataAccessorBuilder() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    // Custom session timeout value is used to count active connections in SharedZkClientFactory
    int customSessionTimeout = 10000;
    String firstZkAddress = ZK_PREFIX + ZK_START_PORT; // has "CLUSTER_1"
    String firstClusterPath = "/CLUSTER_1";
    String secondClusterPath = "/CLUSTER_2";
    ZkBaseDataAccessor.Builder<ZNRecord> zkBaseDataAccessorBuilder =
        new ZkBaseDataAccessor.Builder<>();
    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    connectionConfigBuilder.setSessionTimeout(customSessionTimeout);
    BaseDataAccessor<ZNRecord> accessor;

    // Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address set
    int currentSharedZkClientActiveConnectionCount =
        SharedZkClientFactory.getInstance().getActiveConnectionCount();
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.DEDICATED).setZkAddress(firstZkAddress)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    // Check that no extra connection has been created
    //Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
    //    currentSharedZkClientActiveConnectionCount);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address not set, ZK sharding key set
    connectionConfigBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkRealmShardingKey(firstClusterPath);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.DEDICATED).setZkAddress(null)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
        currentSharedZkClientActiveConnectionCount);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, ZK address set, ZK sharding key set (ZK addr should override)
    connectionConfigBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkRealmShardingKey(secondClusterPath);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.DEDICATED).setZkAddress(firstZkAddress)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
        currentSharedZkClientActiveConnectionCount);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address set
    currentSharedZkClientActiveConnectionCount =
        SharedZkClientFactory.getInstance().getActiveConnectionCount();
    connectionConfigBuilder.setZkRealmShardingKey(null).setRealmMode(null);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.SHARED).setZkAddress(firstZkAddress)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    // Add one to active connection count since this is a shared ZkClientType
    Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
        currentSharedZkClientActiveConnectionCount + 1);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address not set, ZK sharding key set
    connectionConfigBuilder.setZkRealmShardingKey(firstClusterPath)
        .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.SHARED).setZkAddress(null)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    // Add one to active connection count since this is a shared ZkClientType
    Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
        currentSharedZkClientActiveConnectionCount + 1);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, sharedZkClient, ZK address set, ZK sharding key set
    // (ZK address should override the sharding key setting)
    connectionConfigBuilder.setZkRealmShardingKey(secondClusterPath)
        .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
        .setZkClientType(ZkClientType.SHARED).setZkAddress(firstZkAddress)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertFalse(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    // Add one to active connection count since this is a shared ZkClientType
    Assert.assertEquals(SharedZkClientFactory.getInstance().getActiveConnectionCount(),
        currentSharedZkClientActiveConnectionCount + 1);
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, federated ZkClient (should fail)
    connectionConfigBuilder.setZkRealmShardingKey(null).setRealmMode(null);
    try {
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
          .setZkClientType(ZkClientType.FEDERATED).setZkAddress(firstZkAddress)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("SINGLE_REALM and FEDERATED ZkClientType are an invalid combination!");
    } catch (HelixException e) {
      // Expected
    }

    // Create ZkBaseDataAccessor, multi-realm, dedicated ZkClient (should fail)
    try {
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
          .setZkClientType(ZkClientType.DEDICATED).setZkAddress(firstZkAddress)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("MULTI_REALM and DEDICATED ZkClientType are an invalid combination!");
    } catch (HelixException e) {
      // Expected
    }

    // Create ZkBaseDataAccessor, multi-realm, shared ZkClient (should fail)
    try {
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
          .setZkClientType(ZkClientType.SHARED).setZkAddress(firstZkAddress)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("MULTI_REALM and SHARED ZkClientType are an invalid combination!");
    } catch (HelixException e) {
      // Expected
    }

    // Create ZkBaseDataAccessor, multi-realm, federated ZkClient, ZkAddress set (should fail)
    try {
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
          .setZkClientType(ZkClientType.FEDERATED).setZkAddress(firstZkAddress)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("MULTI_REALM and FEDERATED ZkClientType do not connect to one ZK!");
    } catch (HelixException e) {
      // Expected
    }

    // Create ZkBaseDataAccessor, multi-realm, federated ZkClient, Zk sharding key set (should fail
    // because by definition, multi-realm can access multiple sharding keys)
    try {
      connectionConfigBuilder.setZkRealmShardingKey(firstClusterPath)
          .setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM);
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
          .setZkClientType(ZkClientType.FEDERATED).setZkAddress(null)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("MULTI_REALM and FEDERATED ZkClientType do not connect to one ZK!");
    } catch (HelixException e) {
      // Expected
    }

    // Create ZkBaseDataAccessor, multi-realm, federated ZkClient
    connectionConfigBuilder.setZkRealmShardingKey(null).setRealmMode(null);
    accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.MULTI_REALM)
        .setZkClientType(ZkClientType.FEDERATED).setZkAddress(null)
        .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
    Assert.assertTrue(accessor.exists(firstClusterPath, AccessOption.PERSISTENT));
    Assert.assertTrue(accessor.exists(secondClusterPath, AccessOption.PERSISTENT));
    accessor.close();

    // Create ZkBaseDataAccessor, single-realm, dedicated ZkClient, No ZkAddress set,
    // ConnectionConfig has an invalid ZK sharding key (should fail because it cannot find a valid
    // ZK to connect to)
    connectionConfigBuilder.setZkRealmShardingKey("/NonexistentShardingKey");
    try {
      accessor = zkBaseDataAccessorBuilder.setRealmMode(RealmAwareZkClient.RealmMode.SINGLE_REALM)
          .setZkClientType(ZkClientType.DEDICATED).setZkAddress(null)
          .setRealmAwareZkConnectionConfig(connectionConfigBuilder.build()).build();
      Assert.fail("Should fail because it cannot find a valid ZK to connect to!");
    } catch (NoSuchElementException e) {
      // Expected because the sharding key wouldn't be found
    }

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
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
  @Test(dependsOnMethods = "testGenericBaseDataAccessorBuilder")
  public void testDifferentMsdsEndpointConfigs() throws IOException, InvalidRoutingDataException {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    final String zkAddress = ZK_SERVER_MAP.keySet().iterator().next();
    final Map<String, Collection<String>> secondRoutingData =
        ImmutableMap.of(zkAddress, Collections.singletonList(formPath(CLUSTER_FOUR)));
    MockMetadataStoreDirectoryServer secondMsds =
        new MockMetadataStoreDirectoryServer("localhost", 11118, "multiZkTest", secondRoutingData);
    final RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfig =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP.name())
            .setRoutingDataSourceEndpoint(secondMsds.getEndpoint()).build();
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
    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }

  private void verifyHelixManagerMsdsEndpoint() {
    System.out.println("Start " + TestHelper.getTestMethodName());

    // Mock participants are already created and started in the previous test.
    // The mock participant only connects to MSDS configured in system property,
    // but not the other.
    final MockParticipantManager manager = MOCK_PARTICIPANTS.iterator().next();
    verifyMsdsZkRealm(CLUSTER_FOUR, false,
        () -> manager.getZkClient().exists(formPath(CLUSTER_FOUR)));

    verifyMsdsZkRealm(CLUSTER_ONE, true,
        () -> manager.getZkClient().exists(formPath(manager.getClusterName())));
    if (manager.getZkClient() != null) {
      System.out.println("zk client is not yet null2");
    }
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
      verifyMsdsZkRealm(CLUSTER_ONE, true, () -> firstClusterSetup.addCluster(CLUSTER_ONE, false));
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
      verifyMsdsZkRealm(CLUSTER_ONE, true, () -> firstHelixAdmin.enableCluster(CLUSTER_ONE, true));
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
      verifyMsdsZkRealm(CLUSTER_ONE, true, () -> firstConfigAccessor.getClusterConfig(CLUSTER_ONE));
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
        Assert.assertTrue(e.getMessage()
            .startsWith("No sharding key found within the provided path. Path: /" + cluster));
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
    } catch (IllegalStateException e) {
        System.out.println("Exception: " + e);
    }
  }

  private String formPath(String... pathNames) {
    StringBuilder sb = new StringBuilder();
    for (String name : pathNames) {
      sb.append('/').append(name);
    }
    return sb.toString();
  }

  /**
   * Testing using ZK as the routing data source. We use BaseDataAccessor as the representative
   * Helix API.
   * Two modes are tested: ZK and HTTP-ZK fallback
   */
  @Test(dependsOnMethods = "testDifferentMsdsEndpointConfigs")
  public void testZkRoutingDataSourceConfigs() {
    String methodName = TestHelper.getTestMethodName();
    System.out.println("START " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));

    // Set up routing data in ZK by connecting directly to ZK
    BaseDataAccessor<ZNRecord> accessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>().setZkAddress(ZK_PREFIX + ZK_START_PORT).build();

    // Create ZK realm routing data ZNRecord
    _rawRoutingData.forEach((realm, keys) -> {
      ZNRecord znRecord = new ZNRecord(realm);
      znRecord.setListField(MetadataStoreRoutingConstants.ZNRECORD_LIST_FIELD_KEY,
          new ArrayList<>(keys));
      accessor.set(MetadataStoreRoutingConstants.ROUTING_DATA_PATH + "/" + realm, znRecord,
          AccessOption.PERSISTENT);
    });

    // Create connection configs with the source type set to each type
    final RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    final RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfigZk =
        connectionConfigBuilder.setRoutingDataSourceType(RoutingDataReaderType.ZK.name())
            .setRoutingDataSourceEndpoint(ZK_PREFIX + ZK_START_PORT).build();
    final RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfigHttpZkFallback =
        connectionConfigBuilder
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name())
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT).build();
    final RealmAwareZkClient.RealmAwareZkConnectionConfig connectionConfigHttp =
        connectionConfigBuilder.setRoutingDataSourceType(RoutingDataReaderType.HTTP.name())
            .setRoutingDataSourceEndpoint(_msdsEndpoint).build();

    // Reset cached routing data
    RoutingDataManager.getInstance().reset(true);
    // Shutdown MSDS to ensure that these accessors are able to pull routing data from ZK
    _msds.stopServer();

    // Create a BaseDataAccessor instance with the connection config
    BaseDataAccessor<ZNRecord> zkBasedAccessor = new ZkBaseDataAccessor.Builder<ZNRecord>()
        .setRealmAwareZkConnectionConfig(connectionConfigZk).build();
    BaseDataAccessor<ZNRecord> httpZkFallbackBasedAccessor =
        new ZkBaseDataAccessor.Builder<ZNRecord>()
            .setRealmAwareZkConnectionConfig(connectionConfigHttpZkFallback).build();
    try {
      BaseDataAccessor<ZNRecord> httpBasedAccessor = new ZkBaseDataAccessor.Builder<ZNRecord>()
          .setRealmAwareZkConnectionConfig(connectionConfigHttp).build();
      Assert.fail("Must fail with a MultiZkException because HTTP connection will be refused.");
    } catch (MultiZkException e) {
      // Okay
    }

    // Check that all clusters appear as existing to this accessor
    CLUSTER_LIST.forEach(cluster -> {
      Assert.assertTrue(zkBasedAccessor.exists("/" + cluster, AccessOption.PERSISTENT));
      Assert.assertTrue(httpZkFallbackBasedAccessor.exists("/" + cluster, AccessOption.PERSISTENT));
    });

    // Close all connections
    accessor.close();
    zkBasedAccessor.close();
    httpZkFallbackBasedAccessor.close();

    System.out.println("END " + _className + "_" + methodName + " at " + new Date(System.currentTimeMillis()));
  }
}
