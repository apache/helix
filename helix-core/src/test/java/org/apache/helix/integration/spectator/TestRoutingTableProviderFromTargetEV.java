package org.apache.helix.integration.spectator;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.mock.participant.MockDelayMSStateModelFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProviderFromTargetEV extends ZkTestBase {
  private HelixManager _manager;
  private final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants =  new MockParticipantManager[NUM_NODES];
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_PARTITIONS,
        MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.name());

    _gSetupTool
        .rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_REPLICAS);

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // add a delayed state model
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      MockDelayMSStateModelFactory delayFactory =
          new MockDelayMSStateModelFactory().setDelay(-300000L);
      stateMachine.registerStateModelFactory(MASTER_SLAVE_STATE_MODEL, delayFactory);
      _participants[i].syncStart();
    }

    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }

    deleteCluster(CLUSTER_NAME);
  }

  @Test (expectedExceptions = HelixException.class)
  public void testTargetExternalViewWithoutEnable() {
    new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW);
  }

  @Test
  public void testExternalViewDoesNotExist() {
    String resourceName = WorkflowGenerator.DEFAULT_TGT_DB + 1;
    RoutingTableProvider externalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    try {
      Assert.assertEquals(externalViewProvider.getInstancesForResource(resourceName, "SLAVE").size(),
          0);
    } finally {
      externalViewProvider.shutdown();
    }
  }

  @Test (dependsOnMethods = "testTargetExternalViewWithoutEnable")
  public void testExternalViewDiffFromTargetExternalView() throws InterruptedException {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.enableTargetExternalView(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Thread.sleep(2000);

    RoutingTableProvider externalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    RoutingTableProvider targetExternalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW);

    try {
      // ExternalView should not contain any MASTERS
      // TargetExternalView should contain MASTERS same as the partition number
      Set<InstanceConfig> externalViewMasters =
          externalViewProvider.getInstancesForResource(WorkflowGenerator.DEFAULT_TGT_DB, "MASTER");
      Assert.assertEquals(externalViewMasters.size(), 0);
      Set<InstanceConfig> targetExternalViewMasters = targetExternalViewProvider
          .getInstancesForResource(WorkflowGenerator.DEFAULT_TGT_DB, "MASTER");
      Assert.assertEquals(targetExternalViewMasters.size(), NUM_NODES);

      // TargetExternalView MASTERS mapping should exactly match IdealState MASTERS mapping
      Map<String, Map<String, String>> stateMap = _gSetupTool.getClusterManagementTool()
          .getResourceIdealState(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB).getRecord().getMapFields();

      Set<String> idealMasters = new HashSet<>();
      Set<String> targetMasters = new HashSet<>();
      for (Map<String, String> instanceMap : stateMap.values()) {
        for (String instance : instanceMap.keySet()) {
          if (instanceMap.get(instance).equals("MASTER")) {
            idealMasters.add(instance);
          }
        }
      }

      for (InstanceConfig instanceConfig : targetExternalViewMasters) {
        targetMasters.add(instanceConfig.getInstanceName());
      }
      Assert.assertTrue(idealMasters.equals(targetMasters));
    } finally {
      externalViewProvider.shutdown();
      targetExternalViewProvider.shutdown();
    }
  }
}
