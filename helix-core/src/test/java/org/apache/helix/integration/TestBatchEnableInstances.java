package org.apache.helix.integration;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestBatchEnableInstances extends TaskTestBase {
  private ConfigAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numReplicas = 3;
    _numNodes = 5;
    _numParitions = 4;
    super.beforeClass();
    _accessor = new ConfigAccessor(_gZkClient);
  }

  @Test
  public void testOldEnableDisable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), false);
    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numParitions);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(!stateMap.keySet().contains(_participants[0].getInstanceName()));
    }
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), true);
  }

  @Test
  public void testBatchEnableDisable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        false);
    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numParitions);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(!stateMap.keySet().contains(_participants[0].getInstanceName()));
      Assert.assertTrue(!stateMap.keySet().contains(_participants[1].getInstanceName()));
    }
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
  }

  @Test
  public void testOldDisableBatchEnable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), false);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numParitions);
    int numOfFirstHost = 0;
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.keySet().contains(_participants[0].getInstanceName())) {
        numOfFirstHost++;
      }
    }
    Assert.assertTrue(numOfFirstHost > 0);
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), true);
  }

  @Test
  public void testBatchDisableOldEnable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        false);
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), true);
    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numParitions);
    int numOfFirstHost = 0;
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.keySet().contains(_participants[0].getInstanceName())) {
        numOfFirstHost++;
      }
      Assert.assertTrue(!stateMap.keySet().contains(_participants[1].getInstanceName()));
    }
    Assert.assertTrue(numOfFirstHost > 0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
  }
}
