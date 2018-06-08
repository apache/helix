package org.apache.helix.integration.controller;

import java.util.Map;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestClusterMaintenanceMode extends TaskTestBase {
  MockParticipantManager _newInstance;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 3;
    _numReplicas = 3;
    _numParitions = 5;
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
    Thread.sleep(2000);
    ExternalView prevExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + 10);
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    _newInstance =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    _newInstance.syncStart();
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, 3);
    Thread.sleep(3000);
    ExternalView newExternalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(prevExternalView.getRecord().getMapFields(),
        newExternalView.getRecord().getMapFields());
  }

  @Test (dependsOnMethods = "testMaintenanceModeAddNewInstance")
  public void testMaintenanceModeAddNewResource() throws InterruptedException {
    _gSetupTool.getClusterManagementTool()
        .addResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + 1, 7, "MasterSlave",
            IdealState.RebalanceMode.FULL_AUTO.name());
    _gSetupTool.getClusterManagementTool()
        .rebalance(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + 1, 3);
    Thread.sleep(2000);
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB + 1);
    Assert.assertNull(externalView);
  }

  @Test (dependsOnMethods = "testMaintenanceModeAddNewResource")
  public void testMaintenanceModeInstanceDown() throws InterruptedException {
    _participants[0].syncStop();
    Thread.sleep(2000);
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
    Thread.sleep(2000);
    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.containsKey(_participants[0].getInstanceName())) {
        Assert.assertTrue(stateMap.get(_participants[0].getInstanceName()).equals("SLAVE"));
      }
    }
  }
}
