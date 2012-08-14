package com.linkedin.helix.integration;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestEspressoStorageClusterIdealState;
import com.linkedin.helix.model.IdealState;
import com.linkedin.helix.tools.ClusterSetup;

public class TestExpandCluster extends ZkStandAloneCMTestBase
{
@Test
  public void testExpandCluster()
  {
    String DB2 = "TestDB2";
    int partitions = 100;
    int replica = 3;
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB2, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB2, replica);
    
    String DB3 = "TestDB3";

    _setupTool.addResourceToCluster(CLUSTER_NAME, DB3, partitions, STATE_MODEL);
    
    IdealState testDB0 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    IdealState testDB2 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB2);
    IdealState testDB3 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB3);
    
    for (int i = 0; i < 5; i++)
    {
      String storageNodeName = PARTICIPANT_PREFIX + ":" + (27960 + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.expandCluster(CLUSTER_NAME);
    
    IdealState testDB0_1 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    IdealState testDB2_1 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB2);
    IdealState testDB3_1 = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB3);
    
    Map<String, Object> resultOld2 = ClusterSetup.buildInternalIdealState(testDB2);
    Map<String, Object> result2 = ClusterSetup.buildInternalIdealState(testDB2_1);
    
    TestEspressoStorageClusterIdealState.Verify(result2, partitions, replica - 1);

    Double masterKeepRatio = 0.0, slaveKeepRatio = 0.0;
    double[] result = TestEspressoStorageClusterIdealState.compareResult(resultOld2, result2);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(masterKeepRatio > 0.49 && masterKeepRatio < 0.51);
    
    Assert.assertTrue(testDB3_1.getRecord().getListFields().size() == 0);
    
    Map<String, Object> resultOld = ClusterSetup.buildInternalIdealState(testDB0);
    Map<String, Object> resultNew = ClusterSetup.buildInternalIdealState(testDB0_1);
    
    result = TestEspressoStorageClusterIdealState.compareResult(resultOld, resultNew);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(masterKeepRatio > 0.49 && masterKeepRatio < 0.51);
  }
}
