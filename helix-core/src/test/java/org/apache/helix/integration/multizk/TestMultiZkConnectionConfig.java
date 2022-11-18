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
import java.util.Map;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManagerProperty;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.manager.zk.ZKHelixAdmin;
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
public class TestMultiZkConnectionConfig extends TestMultiZkHelixJavaApis {

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
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

  @Override
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

  /**
   * Test creation of HelixManager and makes sure it connects correctly.
   */
  @Test(dependsOnMethods = "testCreateParticipants")
  public void testZKHelixManager() throws Exception {
    super.testZKHelixManager();
  }

  @Override
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
    super.testZKHelixManagerCloudConfig();
  }
}