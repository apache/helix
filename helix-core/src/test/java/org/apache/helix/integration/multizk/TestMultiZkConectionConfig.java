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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixCloudProperty;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.InstanceType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.msdcommon.constant.MetadataStoreRoutingConstants;
import org.apache.helix.msdcommon.mock.MockMetadataStoreDirectoryServer;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
import org.apache.helix.zookeeper.impl.factory.DedicatedZkClientFactory;
import org.apache.helix.zookeeper.routing.RoutingDataManager;
import org.apache.helix.zookeeper.zkclient.ZkServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Some Helix user do not have multizk routing configs in jvm system config but pass
 * in through RealAwareZkConnectionConfig.
 * This test class will not set jvm routing zk config and test Helix functionality.
 * Tests were similar to TestMultiZkHelixJavaApis but without "MSDS_SERVER_ENDPOINT_KEY"
 * in system property
 */
public class TestMultiZkConectionConfig {
  private static Logger LOG = LoggerFactory.getLogger(TestMultiZkConectionConfig.class);
  private static final int NUM_ZK = 3;
  private static final Map<String, ZkServer> ZK_SERVER_MAP = new HashMap<>();
  private static final Map<String, HelixZkClient> ZK_CLIENT_MAP = new HashMap<>();
  private static final Map<String, ClusterControllerManager> MOCK_CONTROLLERS = new HashMap<>();
  private static final Set<MockParticipantManager> MOCK_PARTICIPANTS = new HashSet<>();
  private static final List<String> CLUSTER_LIST =
      ImmutableList.of("CLUSTER_1", "CLUSTER_2", "CLUSTER_3");

  private MockMetadataStoreDirectoryServer _msds;
  private static final Map<String, Collection<String>> _rawRoutingData = new HashMap<>();
  private RealmAwareZkClient _zkClient;
  private HelixAdmin _zkHelixAdmin;

  // Save System property configs from before this test and pass onto after the test
  private final Map<String, String> _configStore = new HashMap<>();

  private static final String ZK_PREFIX = "localhost:";
  private static final int ZK_START_PORT = 8977;
  private String _msdsEndpoint;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Create 3 in-memory zookeepers and routing mapping
    for (int i = 0; i < NUM_ZK; i++) {
      String zkAddress = ZK_PREFIX + (ZK_START_PORT + i);
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
    _msdsEndpoint =
        "http://" + msdsHostName + ":" + msdsPort + "/admin/v2/namespaces/" + msdsNamespace;
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
    // We are not setting routing ZK in system property.
    //System.setProperty(MetadataStoreRoutingConstants.MSDS_SERVER_ENDPOINT_KEY, _msdsEndpoint);

    // Routing data may be set by other tests using the same endpoint; reset() for good measure
    RoutingDataManager.getInstance().reset();
    // Create a FederatedZkClient for admin work

    try {
      _zkClient =
          new FederatedZkClient(new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
              .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
              .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build(),
              new RealmAwareZkClient.RealmAwareZkClientConfig());
      _zkClient.setZkSerializer(new ZNRecordSerializer());
    } catch (Exception ex) {
      for (StackTraceElement elm : ex.getStackTrace()) {
        System.out.println(elm);
      }
    }
    System.out.println("end start");
  }

  @AfterClass
  public void afterClass() throws Exception {
    String testClassName = getClass().getSimpleName();

    try {
      // Kill all mock controllers and participants
      MOCK_CONTROLLERS.values().forEach(ClusterControllerManager::syncStop);
      MOCK_PARTICIPANTS.forEach(mockParticipantManager -> {
        mockParticipantManager.syncStop();
        StateMachineEngine stateMachine = mockParticipantManager.getStateMachineEngine();
        if (stateMachine != null) {
          StateModelFactory stateModelFactory = stateMachine.getStateModelFactory("Task");
          if (stateModelFactory != null && stateModelFactory instanceof TaskStateModelFactory) {
            ((TaskStateModelFactory) stateModelFactory).shutdown();
          }
        }
      });

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
      ZK_CLIENT_MAP.forEach((zkAddress, zkClient) -> zkClient.close());
      ZK_SERVER_MAP.forEach((zkAddress, zkServer) -> zkServer.shutdown());

      // Stop MockMSDS
      _msds.stopServer();

      // Close ZK client connections
      _zkHelixAdmin.close();
      if (_zkClient != null && !_zkClient.isClosed()) {
        _zkClient.close();
      }
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
    ClusterSetup clusterSetupZkAddr = new ClusterSetup(_zkClient);
    ClusterSetup clusterSetupBuilder = new ClusterSetup.Builder().setRealmAwareZkConnectionConfig(
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder()
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name()).build())
        .build();

    createClusters(clusterSetupZkAddr);
    verifyClusterCreation(clusterSetupZkAddr);

    createClusters(clusterSetupBuilder);
    verifyClusterCreation(clusterSetupBuilder);

    // Create clusters again to continue with testing
    createClusters(clusterSetupBuilder);

    clusterSetupZkAddr.close();
    clusterSetupBuilder.close();
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
    //HelixAdmin helixAdminZkAddr = new ZKHelixAdmin(ZK_SERVER_MAP.keySet().iterator().next());
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
    //createParticipantsAndVerify(helixAdminZkAddr, numParticipants, participantNamePrefix);
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

    //helixAdminZkAddr.close();
    helixAdminBuilder.close();
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
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Test(dependsOnMethods = "testCreateParticipants")
  public void testZKHelixManager() throws Exception {
    String clusterName = "CLUSTER_1";
    String participantName = "HelixManager";
    InstanceConfig instanceConfig = new InstanceConfig(participantName);
    _zkHelixAdmin.addInstance(clusterName, instanceConfig);

    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    // Try with a connection config without ZK realm sharding key set (should fail)
    RealmAwareZkClient.RealmAwareZkConnectionConfig invalidZkConnectionConfig =
        connectionConfigBuilder.build();
    RealmAwareZkClient.RealmAwareZkConnectionConfig validZkConnectionConfig =
        connectionConfigBuilder
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name())
            .setZkRealmShardingKey("/" + clusterName).build();
    HelixManagerProperty.Builder propertyBuilder = new HelixManagerProperty.Builder();
    try {
      HelixManager invalidManager = HelixManagerFactory
          .getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, null,
              propertyBuilder.setRealmAWareZkConnectionConfig(invalidZkConnectionConfig).build());
      Assert.fail("Should see a HelixException here because the connection config doesn't have the "
          + "sharding key set!");
    } catch (HelixException e) {
      // Expected
    }

    // Connect as a participant
    HelixManager managerParticipant = HelixManagerFactory
        .getZKHelixManager(clusterName, participantName, InstanceType.PARTICIPANT, null,
            propertyBuilder.setRealmAWareZkConnectionConfig(validZkConnectionConfig).build());
    managerParticipant.connect();

    // Connect as an administrator
    HelixManager managerAdministrator = HelixManagerFactory
        .getZKHelixManager(clusterName, participantName, InstanceType.ADMINISTRATOR, null,
            propertyBuilder.setRealmAWareZkConnectionConfig(validZkConnectionConfig).build());
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
  }

  /**
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Test(dependsOnMethods = "testZKHelixManager")
  public void testZKHelixManagerCloudConfig() throws Exception {
    String clusterName = "CLUSTER_1";
    String participantName = "HelixManager";
    InstanceConfig instanceConfig = new InstanceConfig(participantName);
    _zkHelixAdmin.addInstance(clusterName, instanceConfig);

    RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder connectionConfigBuilder =
        new RealmAwareZkClient.RealmAwareZkConnectionConfig.Builder();
    RealmAwareZkClient.RealmAwareZkConnectionConfig validZkConnectionConfig =
        connectionConfigBuilder
            .setRoutingDataSourceEndpoint(_msdsEndpoint + "," + ZK_PREFIX + ZK_START_PORT)
            .setRoutingDataSourceType(RoutingDataReaderType.HTTP_ZK_FALLBACK.name())
            .setZkRealmShardingKey("/" + clusterName).build();
    HelixManagerProperty.Builder propertyBuilder = new HelixManagerProperty.Builder();

    // create a dummy cloud config and pass to ManagerFactory. It should be overwrite by
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
        propertyBuilder.setRealmAWareZkConnectionConfig(validZkConnectionConfig)
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
  }
}
