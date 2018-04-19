package org.apache.helix.monitoring.mbeans;

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

import java.lang.management.ManagementFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test specifically tests MBean metrics instrumented in ClusterStatusMonitor that aggregate individual
 * resource-level metrics into cluster-level figures.
 *
 * Sets up 3 Participants and 5 partitions with 3 replicas each, the test monitors the change in the numbers
 * when a Participant is disabled.
 *
 */
public class TestClusterAggregateMetrics extends ZkIntegrationTestBase {

  // Configurable values for test setup
  private static final int NUM_PARTICIPANTS = 3;
  private static final int NUM_PARTITIONS = 5;
  private static final int NUM_REPLICAS = 3;

  private static final String PARTITION_COUNT = "TotalPartitionCount";
  private static final String ERROR_PARTITION_COUNT = "TotalErrorPartitionCount";
  private static final String WITHOUT_TOPSTATE_COUNT = "TotalPartitionsWithoutTopStateCount";
  private static final String IS_EV_MISMATCH_COUNT = "TotalExternalViewIdealStateMismatchPartitionCount";

  private static final int START_PORT = 12918;
  private static final String STATE_MODEL = "MasterSlave";
  private static final String TEST_DB = "TestDB";
  private static final MBeanServerConnection _server = ManagementFactory.getPlatformMBeanServer();
  private final String CLASS_NAME = getShortClassName();
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private ClusterSetup _setupTool;
  private HelixManager _manager;
  private MockParticipantManager[] _participants = new MockParticipantManager[NUM_PARTICIPANTS];
  private ClusterControllerManager _controller;
  private Map<String, Object> _beanValueMap = new HashMap<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }
    _setupTool = new ClusterSetup(ZK_ADDR);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, NUM_PARTITIONS, STATE_MODEL);
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, NUM_REPLICAS);

    // start dummy participants
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    // create cluster manager
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
  }

  /**
   * Shutdown order: 1) disconnect the controller 2) disconnect participants.
   *
   */
  @AfterClass
  public void afterClass() {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }

    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testAggregateMetrics() throws InterruptedException {
    // Everything should be up and running initially with 5 total partitions
    updateMetrics();
    Assert.assertEquals(_beanValueMap.get(PARTITION_COUNT), 5L);
    Assert.assertEquals(_beanValueMap.get(ERROR_PARTITION_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(WITHOUT_TOPSTATE_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(IS_EV_MISMATCH_COUNT), 0L);

    // Disable all Participants (instances)
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceName, false);
    }
    Thread.sleep(500);
    updateMetrics();
    Assert.assertEquals(_beanValueMap.get(PARTITION_COUNT), 5L);
    Assert.assertEquals(_beanValueMap.get(ERROR_PARTITION_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(WITHOUT_TOPSTATE_COUNT), 5L);
    Assert.assertEquals(_beanValueMap.get(IS_EV_MISMATCH_COUNT), 5L);

    // Re-enable all Participants (instances)
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceName, true);
    }
    Thread.sleep(500);
    updateMetrics();
    Assert.assertEquals(_beanValueMap.get(PARTITION_COUNT), 5L);
    Assert.assertEquals(_beanValueMap.get(ERROR_PARTITION_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(WITHOUT_TOPSTATE_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(IS_EV_MISMATCH_COUNT), 0L);

    // Drop the resource and check that all metrics are zero.
    _setupTool.dropResourceFromCluster(CLUSTER_NAME, TEST_DB);
    Thread.sleep(500);
    updateMetrics();
    Assert.assertEquals(_beanValueMap.get(PARTITION_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(ERROR_PARTITION_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(WITHOUT_TOPSTATE_COUNT), 0L);
    Assert.assertEquals(_beanValueMap.get(IS_EV_MISMATCH_COUNT), 0L);
  }

  /**
   * Queries for all MBeans from the MBean Server and only looks at the relevant MBean and gets its metric numbers.
   *
   */
  private void updateMetrics() {
    try {
      QueryExp exp = Query.match(Query.attr("SensorName"), Query.value("*" + CLUSTER_NAME + "*"));
      Set<ObjectInstance> mbeans =
          new HashSet<>(ManagementFactory.getPlatformMBeanServer().queryMBeans(new ObjectName("ClusterStatus:*"), exp));
      for (ObjectInstance instance : mbeans) {
        ObjectName beanName = instance.getObjectName();
        if (beanName.toString().equals("ClusterStatus:cluster=" + CLUSTER_NAME)) {
          MBeanInfo info = _server.getMBeanInfo(beanName);
          MBeanAttributeInfo[] infos = info.getAttributes();
          for (MBeanAttributeInfo infoItem : infos) {
            Object val = _server.getAttribute(beanName, infoItem.getName());
            _beanValueMap.put(infoItem.getName(), val);
          }
        }
      }
    } catch (Exception e) {
      // update failed
    }
  }
}
