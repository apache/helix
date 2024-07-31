package org.apache.helix.gateway.base;

import java.util.List;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.gateway.base.manager.ClusterControllerManager;
import org.apache.helix.gateway.base.manager.MockParticipantManager;
import org.apache.helix.gateway.base.util.TestHelper;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.collections.Lists;

public class HelixGatewayTestBase extends ZookeeperTestBase {
  protected static final int START_PORT = 12918;
  protected String _clusterName = "TEST_CLUSTER"; // change the cluster name for each test class
  protected int _numParticipants = 3;
  protected int _numGatewayInstances = 3;
  protected ClusterControllerManager _controller;
  protected List<MockParticipantManager> _participants;
  protected ConfigAccessor _configAccessor;
  protected BestPossibleExternalViewVerifier _clusterVerifier;

  @BeforeClass
  public void beforeClass() {
    _participants = Lists.newArrayList();
    _configAccessor = new ConfigAccessor(ZK_ADDR);
    _gSetupTool.getClusterManagementTool().addCluster(_clusterName, true);
    controllerManagement(true);
    startParticipants();
    startGatewayService();
  }

  @AfterClass
  public void afterClass() {
    controllerManagement(false);
    stopParticipants(true);
    stopGatewayService();
    _gSetupTool.getClusterManagementTool().dropCluster(_clusterName);
  }


  /**
   * Start or stop the controller
   * @param start true to start the controller, false to stop the controller
   */
  private void controllerManagement(boolean start) {
    String controllerName = CONTROLLER_PREFIX + "_0";

    if (start) {
      _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, controllerName);
      _controller.syncStart();

      _clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(_clusterName).setZkClient(_gZkClient).setWaitTillVerify(
              TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();
    } else {
      _controller.syncStop();
    }

    enablePersistBestPossibleAssignment(_gZkClient, _clusterName, start);
  }

  /**
   * Create participants with the given number of participants defined by _numParticipants
   */
  private void startParticipants() {
    for (int i = 0; i < _numParticipants; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(_clusterName, storageNodeName);

      // start dummy participants
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, _clusterName, storageNodeName);
      participant.syncStart();
      _participants.add(participant);
    }
  }

  /**
   * Stop participants and optionally drop the participants
   * if dropParticipants is true
   *
   * @param dropParticipants true to drop the participants, false to stop the participants
   */
  private void stopParticipants(boolean dropParticipants) {
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
      if (dropParticipants) {
        _gSetupTool.getClusterManagementTool().dropInstance(_clusterName,
            _configAccessor.getInstanceConfig(_clusterName, participant.getInstanceName()));
      }
    }
    _participants.clear();
  }

  /**
   * Create a resource with the given number of partitions and replicas
   * WARNING: 1) assume only support OnlineOffline state model
   *          2) assume only support FULL_AUTO rebalance mode
   *
   * Default rebalance strategy is CrushEdRebalanceStrategy
   *
   * @param resourceName    name of the resource
   * @param numPartitions   number of partitions
   * @param numReplicas     number of replicas
   */

  protected void createResource(String resourceName, int numPartitions, int numReplicas) {
    createResource(resourceName, numPartitions, numReplicas, "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");}

  /**
   * Create a resource with the given number of partitions, replicas and rebalance strategy
   *
   * @param resourceName      name of the resource
   * @param numPartitions     number of partitions
   * @param numReplicas       number of replicas
   * @param rebalanceStrategy rebalance strategy
   */
  protected void createResource(String resourceName, int numPartitions, int numReplicas, String rebalanceStrategy) {
    _gSetupTool.getClusterManagementTool().addResource(_clusterName, resourceName, numPartitions, "OnlineOffline",
        "FULL_AUTO", rebalanceStrategy);
    _gSetupTool.getClusterManagementTool().rebalance(_clusterName, resourceName, numReplicas);
  }

  /**
   * Start the gateway service with the given number of gateway instances
   * defined by _numGatewayInstances
   */
  protected void startGatewayService() {
    // Start the gateway service
  }

  /**
   * Stop the gateway service
   */
  protected void stopGatewayService() {
    // Stop the gateway service

  }
}
