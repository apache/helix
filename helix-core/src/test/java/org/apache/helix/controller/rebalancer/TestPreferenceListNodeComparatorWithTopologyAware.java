package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestPreferenceListNodeComparatorWithTopologyAware extends ZkTestBase {
  static final String CLUSTER_NAME = TestHelper.getTestClassName() + "_cluster";
  protected ClusterControllerManager _controller;
  protected List<MockParticipantManager> _participants = new ArrayList<>();
  static final List<String> ZONES = Arrays.asList("zone-0", "zone-1", "zone-2");
  private Map<String, List<String>> _zoneToInstanceMap = new HashMap<>();
  static final int NUM_NODES_PER_ZONE = 3;
  protected static final String ZONE = "zone";
  protected static final String HOST = "host";
  protected static final String LOGICAL_ID = "logicalId";
  protected static final String TOPOLOGY = String.format("%s/%s/%s", ZONE, HOST, LOGICAL_ID);



  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + TestHelper.getTestClassName() + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (String zoneId : ZONES) {
      for (int j = 0; j < NUM_NODES_PER_ZONE; j++) {
        String participantName = PARTICIPANT_PREFIX + "_" + zoneId + "_" + j;
        InstanceConfig instanceConfig = new InstanceConfig.Builder().setDomain(
            String.format("%s=%s, %s=%s, %s=%s", ZONE, zoneId, HOST, participantName, LOGICAL_ID,
                UUID.randomUUID())).setInstanceOperation(InstanceConstants.InstanceOperation.ENABLE)
            .build(participantName);

        _gSetupTool.getClusterManagementTool().addInstance(CLUSTER_NAME, instanceConfig);
        _gSetupTool.getClusterManagementTool().setInstanceZoneId(CLUSTER_NAME, participantName, zoneId);
        // start dummy participants
        MockParticipantManager participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName);
        participant.syncStart();
        _participants.add(participant);

        if (!_zoneToInstanceMap.containsKey(zoneId)) {
          _zoneToInstanceMap.put(zoneId, new ArrayList<>());
        }
        _zoneToInstanceMap.get(zoneId).add(participantName);
      }
    }

    enableTopologyAwareRebalance();
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
  }

  @Test
  public void testPrefrenceListNodeComparator() {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER_NAME);
    cache.refresh(_controller.getHelixDataAccessor());
    StateModelDefinition stateModelDef = cache.getStateModelDef(OnlineOfflineSMD.name);

    // No nodes in preference list, so mz will be considered when sorting
    List<String> preferenceList = Collections.emptyList();

    // All nodes have same state, so state comparator will return 0 if mz representation is identical
    Map<String, String> currentStateMap = new HashMap<>();
    currentStateMap.put(_zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    currentStateMap.put(_zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    currentStateMap.put(_zoneToInstanceMap.get(ZONES.get(1)).get(1), "ONLINE");
    currentStateMap.put(_zoneToInstanceMap.get(ZONES.get(1)).get(2), "ONLINE");
    currentStateMap.put(_zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");

    AbstractRebalancer.PreferenceListNodeComparator comparator =
        new AbstractRebalancer.PreferenceListNodeComparator(currentStateMap, stateModelDef, preferenceList, cache);

    // Replicas in zone 0 should have higher priority than nodes in zone 1, because zone 1 has more replicas
    Assert.assertTrue(comparator.compare(_zoneToInstanceMap.get(ZONES.get(0)).get(0),
        _zoneToInstanceMap.get(ZONES.get(1)).get(0)) < 0);
    Assert.assertTrue(comparator.compare(_zoneToInstanceMap.get(ZONES.get(0)).get(0),
        _zoneToInstanceMap.get(ZONES.get(1)).get(1)) < 0);
    // Similarly, replicas in zone 1 should have lower priority than nodes in zone 2
    Assert.assertTrue(comparator.compare(_zoneToInstanceMap.get(ZONES.get(1)).get(0),
        _zoneToInstanceMap.get(ZONES.get(2)).get(0)) > 0);
    Assert.assertTrue(comparator.compare(_zoneToInstanceMap.get(ZONES.get(1)).get(1),
        _zoneToInstanceMap.get(ZONES.get(2)).get(0)) > 0);
    // Replicas in the same zone should have equal priority
    // Technically this gets ordered by state priority, but all states are the same, so it will also return 0
    Assert.assertEquals(comparator.compare(_zoneToInstanceMap.get(ZONES.get(1)).get(0),
        _zoneToInstanceMap.get(ZONES.get(1)).get(1)), 0);
    Assert.assertEquals(comparator.compare(_zoneToInstanceMap.get(ZONES.get(1)).get(1),
        _zoneToInstanceMap.get(ZONES.get(1)).get(2)), 0);
    // Replicas in zones with equal replica count should have equal priority
    Assert.assertEquals(comparator.compare(_zoneToInstanceMap.get(ZONES.get(0)).get(0),
        _zoneToInstanceMap.get(ZONES.get(2)).get(0)), 0);
  }

  @Test
  public void testComputeBestPossibleStateForPartition() {
    String resourceName = "testResource";
    Partition partition = new Partition("testPartition");
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER_NAME);
    cache.refresh(_controller.getHelixDataAccessor());
    Set<String> liveInstances = cache.getLiveInstances().keySet();
    StateModelDefinition stateModelDef = cache.getStateModelDef(OnlineOfflineSMD.name);
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    Set<String> disabledInstancesForPartition = new HashSet<>();
    IdealState idealState = new IdealState(resourceName);
    idealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    idealState.setRebalancerClassName(WagedRebalancer.class.getName());
    idealState.setReplicas("3");
    idealState.setMinActiveReplicas(2);

    // Create current state with 4 instances, 1 in zone-0, 1 in zone-1, 2 in zone-2
    // The instance in zone-0 is in the preference list
    // The first instance in zone-1 is in the preference list, the second is not
    // The instance in zone-2 is not in the preference list
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(1)).get(1), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(2)).get(1), "ONLINE");

    // Preference list contains first node from each zone (2 current states are not in preference list)
    List<String> preferenceList = Arrays.asList(_zoneToInstanceMap.get(ZONES.get(0)).get(0),
        _zoneToInstanceMap.get(ZONES.get(1)).get(0), _zoneToInstanceMap.get(ZONES.get(2)).get(0));

    // Should preferentially drop the replica from zone-2 that is not in the preference list. This is because another
    // replica already exists in zone-2 but not in zone-1.
    DelayedAutoRebalancer delayedAutoRebalancer = new DelayedAutoRebalancer();
    Map<String, String> result = delayedAutoRebalancer.computeBestPossibleStateForPartition(liveInstances, stateModelDef, preferenceList,
        currentStateOutput, disabledInstancesForPartition, idealState, cache.getClusterConfig(), partition,
        cache.getAbnormalStateResolver(OnlineOfflineSMD.name), cache);

    // Zone-1 replica 1 should be dropped
    Map<String, String> expectedPartitionStates = new HashMap<>();
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(1)).get(1), "DROPPED");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(1), "ONLINE");
    Assert.assertEquals(result, expectedPartitionStates, "Partition movement different than expected");

    // Rebuild current state to drop the replica in zone-1 that was not in preference list
    currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(2)).get(1), "ONLINE");
    result = delayedAutoRebalancer.computeBestPossibleStateForPartition(liveInstances, stateModelDef, preferenceList,
        currentStateOutput, disabledInstancesForPartition, idealState, cache.getClusterConfig(), partition,
        cache.getAbnormalStateResolver(OnlineOfflineSMD.name), cache);

    // Zone-2 replica 0 should now be assigned as it's in the preference list
    expectedPartitionStates = new HashMap<>();
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(1), "ONLINE");
    Assert.assertEquals(result, expectedPartitionStates, "Partition movement different than expected");

    // Rebuild current state to assign Zone-2 replica 0
    currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(2)).get(1), "ONLINE");
    result = delayedAutoRebalancer.computeBestPossibleStateForPartition(liveInstances, stateModelDef, preferenceList,
        currentStateOutput, disabledInstancesForPartition, idealState, cache.getClusterConfig(), partition,
        cache.getAbnormalStateResolver(OnlineOfflineSMD.name), cache);

    // Zone-1 replica 1 should be told to drop as it is last replica no in preference list
    expectedPartitionStates = new HashMap<>();
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(1), "DROPPED");
    Assert.assertEquals(result, expectedPartitionStates, "Partition movement different than expected");

    // Rebuild current state to drop Zone-1 replica 1
    currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(resourceName, partition, _zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    currentStateOutput.setCurrentState(resourceName, partition,  _zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");
    result = delayedAutoRebalancer.computeBestPossibleStateForPartition(liveInstances, stateModelDef, preferenceList,
        currentStateOutput, disabledInstancesForPartition, idealState, cache.getClusterConfig(), partition,
        cache.getAbnormalStateResolver(OnlineOfflineSMD.name), cache);

    // Expect no movement from the currentState
    expectedPartitionStates = new HashMap<>();
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(0)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(1)).get(0), "ONLINE");
    expectedPartitionStates.put(_zoneToInstanceMap.get(ZONES.get(2)).get(0), "ONLINE");
    Assert.assertEquals(result, expectedPartitionStates, "Partition movement different than expected");
  }

  private void enableTopologyAwareRebalance() {
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTopology(TOPOLOGY);
    clusterConfig.setFaultZoneType(ZONE);
    clusterConfig.setTopologyAwareEnabled(true);
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }
}
