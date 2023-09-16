package org.apache.helix.common.execution;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.testng.Assert;


/**
 * A generic Helix Test Execution Pipeline, which streamlines a lot of code-blocks.
 */
public final class TestExecutionFlow {

  private static final int PARTICIPANT_PORT = 12918;
  private final HelixZkClient _helixZkClient;

  private TestClusterParameters _clusterParameters;
  private ClusterControllerManager _controller;
  private MockParticipantManager[] _participants;
  private BestPossibleExternalViewVerifier _bestPossibleExternalViewVerifier;

  public TestExecutionFlow(HelixZkClient zkClient) {
    _helixZkClient = zkClient;
  }

  public String getClusterName() {
    return _clusterParameters.getClusterName();
  }

  public ClusterControllerManager getController() {
    return _controller;
  }

  public MockParticipantManager[] getParticipants() {
    return _participants;
  }

  public TestExecutionFlow createCluster(TestClusterParameters parameters) {
    if (parameters == null) {
        throw new RuntimeException("Please specify Cluster creation parameters.");
    }

    _clusterParameters = parameters;
    _participants = new MockParticipantManager[_clusterParameters.getNodeCount()];
    _bestPossibleExternalViewVerifier = new BestPossibleExternalViewVerifier.Builder(_clusterParameters.getClusterName())
        .setZkClient(_helixZkClient)
        .setZkAddress(TestExecutionRuntime.ZK_ADDR)
        .build();

    // setup cluster.
    TestHelper.setupCluster(
        _clusterParameters.getClusterName(),
        TestExecutionRuntime.ZK_ADDR,
        PARTICIPANT_PORT,
        "localhost", // participant name prefix
        ObjectUtils.defaultIfNull(_clusterParameters.getResourceName(), "TestDB"), // resource name prefix
        _clusterParameters.getResourcesCount(),
        _clusterParameters.getPartitionsPerResource(),
        _clusterParameters.getNodeCount(),
        _clusterParameters.getReplicaCount(),
        _clusterParameters.getStateModelDef(),
        _clusterParameters.isRebalanced());
    return this;
  }

  public TestExecutionFlow startController() {
    _controller = new ClusterControllerManager(
        TestExecutionRuntime.ZK_ADDR,
        _clusterParameters.getClusterName(),
        String.format("controller_0_%s", RandomStringUtils.random(10)));
    _controller.syncStart();
    return this;
  }

  public TestExecutionFlow initializeParticipants() {
    initializeParticipants(() -> {});
    return this;
  }

  public TestExecutionFlow initializeParticipants(Runnable participantInitHook) {
    for (int i = 0; i < _clusterParameters.getNodeCount(); i ++) {
      _participants[i] = new MockParticipantManager(
          TestExecutionRuntime.ZK_ADDR,
          _clusterParameters.getClusterName(),
          "localhost_" + (PARTICIPANT_PORT + i));
      participantInitHook.run();
      _participants[i].syncStart();
    }
    return this;
  }

  public TestExecutionFlow assertBestPossibleExternalViewVerifier() {
    Assert.assertTrue(_bestPossibleExternalViewVerifier.verifyByZkCallback(30000));
    return this;
  }

  public TestExecutionFlow cleanup() {
    _controller.syncStop();
    for (MockParticipantManager participant : _participants) {
      participant.syncStop();
    }
    TestHelper.dropCluster(_clusterParameters.getClusterName(), _helixZkClient);
    return this;
  }

}
