package org.apache.helix.integration;

import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.CustomSemiAutoRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateProperty;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestCustomSemiAutoRebalancer extends ZkStandAloneCMTestBase {

  private final int _replica = 2;
  private final int _PARTITIONS = 4;
  private final int NODE_NR = 3;
  
  private HelixDataAccessor accessor;
  
  String getInstanceName(int idx) {
    return String.format("%s_%d", PARTICIPANT_PREFIX, idx);
  }
  
  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
    
    _setupTool = new ClusterSetup(ZK_ADDR);

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }
    _setupTool = new ClusterSetup(_gZkClient);

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, "MasterSlave");
    _setupTool.addResourceProperty(CLUSTER_NAME, TEST_DB,
      IdealStateProperty.REBALANCER_CLASS_NAME.toString(),
      CustomSemiAutoRebalancer.class.getName());
    _setupTool.addResourceProperty(CLUSTER_NAME, TEST_DB, IdealStateProperty.REBALANCE_MODE.toString(),
      RebalanceMode.USER_DEFINED.toString());
    _setupTool.addResourceProperty(CLUSTER_NAME, TEST_DB, IdealStateProperty.REPLICAS.toString(), 
      Integer.toString(_replica));
    
    for (int i = 0; i < NODE_NR; i++) {
      _setupTool.addInstanceToCluster(CLUSTER_NAME, getInstanceName(i));
    }

    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    ZNRecord znRecord = idealState.getRecord();
    for (int i = 0; i < _PARTITIONS / 2; i++) {
      znRecord.setListField(String.format("%s_%d", TEST_DB, i), ImmutableList.of(getInstanceName(0), getInstanceName(1)));
    }
    for (int i = _PARTITIONS / 2; i < _PARTITIONS; i++) {
      znRecord.setListField(String.format("%s_%d", TEST_DB, i), ImmutableList.of(getInstanceName(1), getInstanceName(0)));
    }
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, new IdealState(znRecord));
    
    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      MockParticipantManager participant = new MockParticipantManager(
          ZK_ADDR, CLUSTER_NAME, getInstanceName(i));
      participant.syncStart();
      _participants[i] = participant;
    }

    // start controller
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX + "_0");
    _controller.syncStart();
    
    accessor = new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME));
    ZNRecord extView = getExternalView();

    Assert.assertTrue(result);
  }
  
  @AfterClass
  @Override
  public void afterClass() throws Exception {
    // shutdown order: 1) disconnect the controller 2) disconnect participants
    _controller.syncStop();
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }
    System.out.println("END " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
  }
  
  private ZNRecord getExternalView() {
    return accessor.getProperty(accessor.keyBuilder().externalView(TEST_DB)).getRecord();
  }
  
  @Test
  public void testCustomSemiAutoRebalancer() throws InterruptedException {
    ZNRecord extViewRecord = null;
    Map<String, String> extView = null;
    
    // first stop one instance
    _participants[0].syncStop();
    Thread.sleep(1000L);
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME)));
    
    // add node back
    _participants[0] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, getInstanceName(0));
    _participants[0].syncStart();
    Thread.sleep(1000L);
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME)));
    
    // swap instance 
    IdealState idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    ZNRecord znRecord = idealState.getRecord();
    znRecord.setListField(String.format("%s_%d",  TEST_DB, 0), ImmutableList.of(getInstanceName(0), getInstanceName(2)));
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, new IdealState(znRecord));
    Thread.sleep(1000L);
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME)));
    extViewRecord = getExternalView();
    extView = extViewRecord.getMapField(String.format("%s_%d", TEST_DB, 0));
    Assert.assertEquals(extView.get(getInstanceName(0)), "MASTER");
    Assert.assertEquals(extView.get(getInstanceName(2)), "SLAVE");
    znRecord.setListField(String.format("%s_%d",  TEST_DB, 0), ImmutableList.of(getInstanceName(0), getInstanceName(1)));
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, new IdealState(znRecord));
    
    // change mapping field
    idealState = _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    znRecord = idealState.getRecord();
    znRecord.setMapField(String.format("%s_%d", TEST_DB, 0), ImmutableMap.of(getInstanceName(0), "SLAVE"));
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, new IdealState(znRecord));
    Thread.sleep(1000L);
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME)));
    extViewRecord = getExternalView();
    extView = extViewRecord.getMapField(String.format("%s_%d", TEST_DB, 0));
    Assert.assertEquals(extView.get(getInstanceName(0)), "SLAVE");
    Assert.assertEquals(extView.get(getInstanceName(1)), "MASTER");
  
    znRecord.setMapField(String.format("%s_%d", TEST_DB, 0), Collections.EMPTY_MAP);
    _setupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, TEST_DB, new IdealState(znRecord));
    Thread.sleep(1000L);
    Assert.assertTrue(ClusterStateVerifier.verifyByZkCallback(
        new MasterNbInExtViewVerifier(ZK_ADDR, CLUSTER_NAME)));  
    extViewRecord = getExternalView();
    extView = extViewRecord.getMapField(String.format("%s_%d", TEST_DB, 0));
    Assert.assertEquals(extView.get(getInstanceName(0)), "MASTER");
    Assert.assertEquals(extView.get(getInstanceName(1)), "SLAVE");
  }
}
