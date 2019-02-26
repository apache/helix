package org.apache.helix.integration.controller;

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

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MaintenanceSignal;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterMaintenanceMode extends TaskTestBase {
  private MockParticipantManager _newInstance;
  private String newResourceAddedDuringMaintenanceMode =
      String.format("%s_%s", WorkflowGenerator.DEFAULT_TGT_DB, 1);

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 3;
    _numReplicas = 3;
    _numPartitions = 5;
    super.beforeClass();
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_newInstance != null && _newInstance.isConnected()) {
      _newInstance.syncStop();
    }
    super.afterClass();
  }

  @Test
  public void testMaintenanceModeAddNewInstance() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableMaintenanceMode(CLUSTER_NAME, true, "Test");
    ExternalView prevExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + 10);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    _newInstance =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    _newInstance.syncStart();
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, 3);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView newExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(prevExternalView.getRecord().getMapFields(),
        newExternalView.getRecord().getMapFields());
  }

  @Test (dependsOnMethods = "testMaintenanceModeAddNewInstance")
  public void testMaintenanceModeAddNewResource() throws InterruptedException {
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode, 7, "MasterSlave",
            IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode, 3);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode);
    Assert.assertNull(externalView);
  }

  @Test (dependsOnMethods = "testMaintenanceModeAddNewResource")
  public void testMaintenanceModeInstanceDown() throws InterruptedException {
    _participants[0].syncStop();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(stateMap.values().contains("MASTER"));
    }
  }

  @Test (dependsOnMethods = "testMaintenanceModeInstanceDown")
  public void testMaintenanceModeInstanceBack() throws InterruptedException {
    _participants[0] =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _participants[0].getInstanceName());
    _participants[0].syncStart();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.containsKey(_participants[0].getInstanceName())) {
        Assert.assertTrue(stateMap.get(_participants[0].getInstanceName()).equals("SLAVE"));
      }
    }
  }

  @Test (dependsOnMethods = "testMaintenanceModeInstanceBack")
  public void testExitMaintenanceModeNewResourceRecovery() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableMaintenanceMode(CLUSTER_NAME, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, newResourceAddedDuringMaintenanceMode);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), 7);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(stateMap.values().contains("MASTER"));
    }
  }

  /**
   * Test that the auto-exit functionality works.
   */
  @Test(dependsOnMethods = "testExitMaintenanceModeNewResourceRecovery")
  public void testAutoExitMaintenanceMode() throws InterruptedException {
    // Set the config for auto-exiting maintenance mode
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.setMaxOfflineInstancesAllowed(2);
    clusterConfig.setNumOfflineInstancesForAutoExit(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Kill 3 instances
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(500L);

    // Check that the cluster is in maintenance
    MaintenanceSignal maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);

    // Now bring up 2 instances
    for (int i = 0; i < 2; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    Thread.sleep(500L);

    // Check that the cluster is no longer in maintenance (auto-recovered)
    maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);
  }

  @Test(dependsOnMethods = "testAutoExitMaintenanceMode")
  public void testNoAutoExitWhenManuallyPutInMaintenance() throws InterruptedException {
    // Manually put the cluster in maintenance
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null, null);

    // Kill 2 instances, which makes it a total of 3 down instances
    for (int i = 0; i < 2; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(500L);

    // Now bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    Thread.sleep(500L);

    // The cluster should still be in maintenance because it was enabled manually
    MaintenanceSignal maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
  }

  /**
   * Test that manual triggering of maintenance mode overrides auto-enabled maintenance.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testNoAutoExitWhenManuallyPutInMaintenance")
  public void testManualEnablingOverridesAutoEnabling() throws InterruptedException {
    // Exit maintenance mode manually
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null, null);

    // Kill 3 instances, which would put cluster in maintenance automatically
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(500L);

    // Check that maintenance signal was triggered by Controller
    MaintenanceSignal maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);

    // Manually enable maintenance mode with customFields
    Map<String, String> customFields = ImmutableMap.of("LDAP", "hulee", "JIRA", "HELIX-999",
        "TRIGGERED_BY", "SHOULD NOT BE RECORDED");
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, true, null,
        customFields);
    Thread.sleep(500L);

    // Check that maintenance mode has successfully overwritten with the right TRIGGERED_BY field
    maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.USER);
    for (Map.Entry<String, String> entry : customFields.entrySet()) {
      if (entry.getKey().equals("TRIGGERED_BY")) {
        Assert.assertEquals(maintenanceSignal.getRecord().getSimpleField(entry.getKey()), "USER");
      } else {
        Assert.assertEquals(maintenanceSignal.getRecord().getSimpleField(entry.getKey()),
            entry.getValue());
      }
    }
  }

  /**
   * Test that maxNumPartitionPerInstance still applies (if any Participant has more replicas than
   * the threshold, the cluster should not auto-exit maintenance mode).
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testManualEnablingOverridesAutoEnabling")
  public void testMaxPartitionLimit() throws InterruptedException {
    // Manually exit maintenance mode
    _gSetupTool.getClusterManagementTool().manuallyEnableMaintenanceMode(CLUSTER_NAME, false, null,
        null);
    Thread.sleep(500L);

    // Since 3 instances are missing, the cluster should have gone back under maintenance
    // automatically
    MaintenanceSignal maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED);

    // Bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    Thread.sleep(500L);

    // Check that the cluster exited maintenance
    maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNull(maintenanceSignal);

    // Kill 3 instances, which would put cluster in maintenance automatically
    for (int i = 0; i < 3; i++) {
      _participants[i].syncStop();
    }
    Thread.sleep(500L);

    // Check that cluster is back under maintenance
    maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_OFFLINE_INSTANCES_EXCEEDED);

    // Set the cluster config for auto-exiting maintenance mode
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    // Setting MaxPartitionsPerInstance to 1 will prevent the cluster from exiting maintenance mode
    // automatically because the instances currently have more than 1
    clusterConfig.setMaxPartitionsPerInstance(1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);
    Thread.sleep(500L);

    // Now bring up all instances
    for (int i = 0; i < 3; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }
    Thread.sleep(500L);

    // Check that the cluster is still in maintenance (should not have auto-exited because it would
    // fail the MaxPartitionsPerInstance check)
    maintenanceSignal = _manager.getHelixDataAccessor()
        .getProperty(_manager.getHelixDataAccessor().keyBuilder().maintenance());
    Assert.assertNotNull(maintenanceSignal);
    Assert.assertEquals(maintenanceSignal.getTriggeringEntity(),
        MaintenanceSignal.TriggeringEntity.CONTROLLER);
    Assert.assertEquals(maintenanceSignal.getAutoTriggerReason(),
        MaintenanceSignal.AutoTriggerReason.MAX_PARTITION_PER_INSTANCE_EXCEEDED);
  }
}
