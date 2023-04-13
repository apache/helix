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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Date;

import org.apache.helix.*;
import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.manager.zk.HelixManagerStateListener;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.model.CloudConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.constant.RoutingDataReaderType;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.FederatedZkClient;
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
  private static String _className = TestHelper.getTestClassName();

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    // Routing data may be set by other tests using the same endpoint; reset() for good measure
    RoutingDataManager.getInstance().reset(true);
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
  @Test(dependsOnMethods = "testCreateParticipants", enabled = false)
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
  @Test(dependsOnMethods = "testZKHelixManager", enabled = false)
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
}
