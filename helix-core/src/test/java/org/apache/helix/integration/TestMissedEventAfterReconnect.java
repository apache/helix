package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.zkclient.ZkClient;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestMissedEventAfterReconnect extends ZkTestBase  {

  protected static String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  protected static int PARTICIPANT_COUNT = 10;
  protected List<MockParticipantManager> _participants = new ArrayList<>();
  protected ClusterControllerManager _controller;
  protected ConfigAccessor _configAccessor;
  protected static int HELIX_MANAGER_TIMEOUT = 11 * 1000; //11 seconds

  @BeforeClass
  public void beforeClass() {
    System.out.println("Start test " + TestHelper.getTestClassName());
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < PARTICIPANT_COUNT; i++) {
      addParticipant("localhost_" + i);
    }

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig =  _configAccessor.getClusterConfig(CLUSTER_NAME);
    String testCapacityKey = "TestCapacityKey";
    clusterConfig.setInstanceCapacityKeys(Collections.singletonList(testCapacityKey));
    clusterConfig.setDefaultInstanceCapacityMap(Collections.singletonMap(testCapacityKey, 100));
    clusterConfig.setDefaultPartitionWeightMap(Collections.singletonMap(testCapacityKey, 1));
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @Test
  public void testControllerDisconnectedDuringEvent() throws Exception {
    System.out.println("Start testControllerDisconnectedDuringEvent at " + new Date(System.currentTimeMillis()));
    String firstDB = "firstDB";
    int numPartition = 10;
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, firstDB, numPartition, "LeaderStandby",
        IdealState.RebalanceMode.FULL_AUTO.name(), null);

    IdealState idealStateOne =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, firstDB);
    idealStateOne.setMinActiveReplicas(2);
    idealStateOne.setRebalancerClassName(WagedRebalancer.class.getName());
    _gSetupTool.getClusterManagementTool().setResourceIdealState(CLUSTER_NAME, firstDB, idealStateOne);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, firstDB, 3);

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME).build();

    Assert.assertTrue(verifier.verifyByPolling());
    MockParticipantManager participantToKill = _participants.get(0);

    //disconnect
    System.out.println("Disconnecting");
    ZkClient helixManagerZkClient = (ZkClient) _controller.getZkClient();
    WatchedEvent event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null);
    helixManagerZkClient.process(event);

    System.out.println("Killing participant 0");
    participantToKill.syncStop();

    System.out.println("Sleeping for timeout");
    Thread.sleep(HELIX_MANAGER_TIMEOUT);

    System.out.println("Reconnecting");
    //reconnect
    event = new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null);
    helixManagerZkClient.process(event);


    Assert.assertTrue(verifier.verifyByPolling());
    Assert.assertFalse(_gZkClient.exists(_controller.getHelixDataAccessor().keyBuilder()
            .liveInstance(participantToKill.getInstanceName()).getPath()), "Instance should be offline from ZK View");
    Assert.assertFalse(hasAssignment(participantToKill.getInstanceName()), "Should not have assignments after reconnect");
    Assert.assertTrue(hasAssignment(_participants.get(1).getInstanceName()), "Should have assignments after reconnect");
    System.out.println("End testControllerDisconnectedDuringEvent at " + new Date(System.currentTimeMillis()));
  }

  private boolean hasAssignment(String instanceName) {
    for (ExternalView ev : getEVs().values()) {
      for (String partition : ev.getPartitionSet()) {
        Map<String, String> stateMap = ev.getStateMap(partition);
        if (stateMap.containsKey(instanceName)) {
          System.out.println("Found assignment for " + instanceName + " with partition " + partition);
          return true;
        }
      }
    }
    System.out.println("No assignment for " + instanceName);
    return false;
  }

  private Map<String, ExternalView> getEVs() {
    Map<String, ExternalView> externalViews = new HashMap<String, ExternalView>();
    for (String db : _gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)) {
      ExternalView ev = _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, db);
      externalViews.put(db, ev);
    }
    return externalViews;
  }

  public MockParticipantManager addParticipant(String instanceName) {
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    _participants.add(participant);
    return participant;
  }

  public void dropParticipant(MockParticipantManager participantToDrop) {
    participantToDrop.syncStop();
    _gSetupTool.getClusterManagementTool().dropInstance(CLUSTER_NAME,
        _gSetupTool.getClusterManagementTool().getInstanceConfig(CLUSTER_NAME, participantToDrop.getInstanceName()));
    _participants.remove(participantToDrop);
  }
}
