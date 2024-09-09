package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestAddResourceWhenNodesOffline extends ZkTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestForceKillInstance.class);
  protected final String CLASS_NAME = getShortClassName();
  protected final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private static final int NUM_REPLICAS = 3;
  private static final int NUM_PARTITIONS = 10;
  private static final int NUM_MIN_ACTIVE_REPLICAS = 2;
  private static final int NUM_NODES = 5;
  private BestPossibleExternalViewVerifier _bestPossibleClusterVerifier;
  private HelixAdmin _admin;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private ClusterControllerManager _controller;
  private ArrayList<String> _resources = new ArrayList<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    enablePersistIntermediateAssignment(_gZkClient, CLUSTER_NAME, true);

    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setRebalanceDelayTime(2000000L);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = "localhost_" + i;
      addParticipant(CLUSTER_NAME, instanceName);
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _admin = new ZKHelixAdmin(_gZkClient);

    _bestPossibleClusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
  }

  @Test
  public void testAddResourceNewCluster() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    String resourceName = "TestWagedDB";
    _resources.add(resourceName);
    createResourceWithWagedRebalance(CLUSTER_NAME, resourceName, "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS,
        NUM_MIN_ACTIVE_REPLICAS);

    Assert.assertTrue(_bestPossibleClusterVerifier.verify());
    Assert.assertTrue(isMinActiveSatisfied());
  }

  @Test (dependsOnMethods = "testAddResourceNewCluster")
  public void testAddResourceWhenInstancesDisabledWithinWindow() {
    System.out.println("START " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));

    // Disable 2 instances
    for (int i = 0; i < 2; i++) {
      _admin.setInstanceOperation(CLUSTER_NAME, _participants.get(i).getInstanceName(),
          InstanceConstants.InstanceOperation.DISABLE);
    }
    Assert.assertTrue(_bestPossibleClusterVerifier.verify());
    Assert.assertTrue(isMinActiveSatisfied());

    // Add resource
    String resourceName = "TestWagedDB2";
    _resources.add(resourceName);
    createResourceWithWagedRebalance(CLUSTER_NAME, resourceName, "MasterSlave", NUM_PARTITIONS, NUM_REPLICAS,
        NUM_MIN_ACTIVE_REPLICAS);

    Assert.assertTrue(_bestPossibleClusterVerifier.verify());
    System.out.println("END " + TestHelper.getTestClassName() + "." + TestHelper.getTestMethodName() + " at "
        + new Date(System.currentTimeMillis()));
  }

  private MockParticipantManager addParticipant(String cluster, String instanceName) {
    _gSetupTool.addInstanceToCluster(cluster, instanceName);
    MockParticipantManager toAddParticipant =
        new MockParticipantManager(ZK_ADDR, cluster, instanceName);
    _participants.add(toAddParticipant);
    toAddParticipant.syncStart();
    return toAddParticipant;
  }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String resource : _resources) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, resource);
      externalViews.put(resource, ev);
    }
    return externalViews;
  }

  private boolean isMinActiveSatisfied() {
    Map<String, ExternalView> externalViews = getEVs();
    for (ExternalView ev : externalViews.values()) {
      Assert.assertEquals(ev.getPartitionSet().size(), NUM_PARTITIONS);
      for (String partition : ev.getPartitionSet()) {
        long activeReplicas = ev.getStateMap(partition).values().stream()
            .filter(state -> state.equals("SLAVE") || state.equals("MASTER"))
            .count();
        if (activeReplicas < NUM_MIN_ACTIVE_REPLICAS) {
          return false;
        }
      }
    }
    return true;
  }
}
