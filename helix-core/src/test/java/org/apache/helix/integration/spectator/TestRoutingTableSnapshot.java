package org.apache.helix.integration.spectator;

import java.util.List;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.integration.common.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestRoutingTableSnapshot extends ZkIntegrationTestBase {
  private HelixManager _manager;
  private ClusterSetup _setupTool;
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    _participants =  new MockParticipantManager[NUM_NODES];
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);

    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @AfterClass
  public void afterClass() throws Exception {
    _manager.disconnect();
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].reset();
      }
    }
  }

  @Test
  public void testRoutingTableSnapshot() throws InterruptedException {
    RoutingTableProvider routingTableProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);

    String db1 = "TestDB-1";
    _setupTool.addResourceToCluster(CLUSTER_NAME, db1, NUM_PARTITIONS, "MasterSlave",
        IdealState.RebalanceMode.FULL_AUTO.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db1, NUM_REPLICAS);

    Thread.sleep(200);
    HelixClusterVerifier clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(clusterVerifier.verify());

    IdealState idealState1 = _setupTool.getClusterManagementTool().getResourceIdealState(
        CLUSTER_NAME, db1);

    RoutingTableSnapshot routingTableSnapshot = routingTableProvider.getRoutingTableSnapshot();
    validateMapping(idealState1, routingTableSnapshot);

    Assert.assertEquals(routingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
    Assert.assertEquals(routingTableSnapshot.getResources().size(), 1);
    Assert.assertEquals(routingTableSnapshot.getLiveInstances().size(), NUM_NODES);

    // add new DB and shutdown an instance
    String db2 = "TestDB-2";
    _setupTool.addResourceToCluster(CLUSTER_NAME, db2, NUM_PARTITIONS, "MasterSlave",
        IdealState.RebalanceMode.FULL_AUTO.name());
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, NUM_REPLICAS);

    // shutdown an instance
    _participants[0].syncStop();
    Thread.sleep(200);
    Assert.assertTrue(clusterVerifier.verify());

    // the original snapshot should not change
    Assert.assertEquals(routingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
    Assert.assertEquals(routingTableSnapshot.getResources().size(), 1);
    Assert.assertEquals(routingTableSnapshot.getLiveInstances().size(), NUM_NODES);

    RoutingTableSnapshot newRoutingTableSnapshot = routingTableProvider.getRoutingTableSnapshot();

    Assert.assertEquals(newRoutingTableSnapshot.getInstanceConfigs().size(), NUM_NODES);
    Assert.assertEquals(newRoutingTableSnapshot.getResources().size(), 2);
    Assert.assertEquals(newRoutingTableSnapshot.getLiveInstances().size(), NUM_NODES - 1);
  }

  private void validateMapping(IdealState idealState, RoutingTableSnapshot routingTableSnapshot) {
    String db = idealState.getResourceName();
    Set<String> partitions = idealState.getPartitionSet();
    for (String partition : partitions) {
      List<InstanceConfig> masterInsEv =
          routingTableSnapshot.getInstancesForResource(db, partition, "MASTER");
      Assert.assertEquals(masterInsEv.size(), 1);

      List<InstanceConfig> slaveInsEv =
          routingTableSnapshot.getInstancesForResource(db, partition, "SLAVE");
      Assert.assertEquals(slaveInsEv.size(), 2);
    }
  }
}

