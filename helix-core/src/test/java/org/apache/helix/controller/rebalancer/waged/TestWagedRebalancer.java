package org.apache.helix.controller.rebalancer.waged;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.constraints.MockRebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AbstractTestClusterModel;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TestWagedRebalancer extends AbstractTestClusterModel {
  private Set<String> _instances;
  private MockRebalanceAlgorithm _algorithm;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _instances = new HashSet<>();
    _instances.add(_testInstanceId);
    _algorithm = new MockRebalanceAlgorithm();
  }

  @Override
  protected ResourceControllerDataProvider setupClusterDataCache() throws IOException {
    ResourceControllerDataProvider testCache = super.setupClusterDataCache();

    // Set up mock idealstate
    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState is = new IdealState(resource);
      is.setNumPartitions(_partitionNames.size());
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas("3");
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _partitionNames.stream()
          .forEach(partition -> is.setPreferenceList(partition, Collections.emptyList()));
      isMap.put(resource, is);
    }
    when(testCache.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(testCache.getIdealStates()).thenReturn(isMap);

    // Set up 2 more instances
    for (int i = 1; i < 3; i++) {
      String instanceName = _testInstanceId + i;
      _instances.add(instanceName);
      // 1. Set up the default instance information with capacity configuration.
      InstanceConfig testInstanceConfig = createMockInstanceConfig(instanceName);
      Map<String, InstanceConfig> instanceConfigMap = testCache.getInstanceConfigMap();
      instanceConfigMap.put(instanceName, testInstanceConfig);
      when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);
      // 2. Mock the live instance node for the default instance.
      LiveInstance testLiveInstance = createMockLiveInstance(instanceName);
      Map<String, LiveInstance> liveInstanceMap = testCache.getLiveInstances();
      liveInstanceMap.put(instanceName, testLiveInstance);
      when(testCache.getLiveInstances()).thenReturn(liveInstanceMap);
      when(testCache.getEnabledInstances()).thenReturn(liveInstanceMap.keySet());
      when(testCache.getEnabledLiveInstances()).thenReturn(liveInstanceMap.keySet());
    }

    return testCache;
  }

  @Test
  public void testRebalance() throws IOException, HelixRebalanceException {
    // Init mock metadatastore for the unit test
    MockAssignmentMetadataStore metadataStore = new MockAssignmentMetadataStore();
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm);

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    // Since there is no special condition, the calculated IdealStates should be exactly the same
    // as the mock algorithm result.
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testPartialRebalance() throws IOException, HelixRebalanceException {
    // Init mock metadatastore for the unit test
    MockAssignmentMetadataStore metadataStore = new MockAssignmentMetadataStore();
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm);

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));

    // Test with partial resources listed in the resourceMap input.
    // Remove the first resource from the input. Note it still exists in the cluster data cache.
    metadataStore.clearMetadataStore();
    resourceMap.remove(_resourceNames.get(0));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testRebalanceWithCurrentState() throws IOException, HelixRebalanceException {
    // Init mock metadatastore for the unit test
    MockAssignmentMetadataStore metadataStore = new MockAssignmentMetadataStore();
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm);

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));

    // Test with current state exists, so the rebalancer should calculate for the intermediate state
    // Create current state based on the cluster data cache.
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (String instanceName : _instances) {
      for (Map.Entry<String, CurrentState> csEntry : clusterData
          .getCurrentState(instanceName, _sessionId).entrySet()) {
        String resourceName = csEntry.getKey();
        CurrentState cs = csEntry.getValue();
        for (Map.Entry<String, String> partitionStateEntry : cs.getPartitionStateMap().entrySet()) {
          currentStateOutput
              .setCurrentState(resourceName, new Partition(partitionStateEntry.getKey()),
                  instanceName, partitionStateEntry.getValue());
        }
      }
    }

    // The state calculation will be adjusted based on the current state.
    // So test the following cases:
    // 1.1. Disable a resource, and the partitions in CS will be offline.
    String disabledResourceName = _resourceNames.get(0);
    clusterData.getIdealState(disabledResourceName).enable(false);
    // 1.2. Adding more unknown partitions to the CS, so they will be dropped.
    String droppingResourceName = _resourceNames.get(1);
    String droppingPartitionName = "UnknownPartition";
    String droppingFromInstance = _testInstanceId;
    currentStateOutput.setCurrentState(droppingResourceName, new Partition(droppingPartitionName),
        droppingFromInstance, "SLAVE");
    resourceMap.get(droppingResourceName).addPartition(droppingPartitionName);

    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, currentStateOutput);
    // All the replica state should be OFFLINE
    IdealState disabledIdealState = newIdealStates.get(disabledResourceName);
    for (String partition : disabledIdealState.getPartitionSet()) {
      Assert.assertTrue(disabledIdealState.getInstanceStateMap(partition).values().stream()
          .allMatch(state -> state.equals("OFFLINE")));
    }
    // the dropped partition should be dropped.
    IdealState droppedIdealState = newIdealStates.get(droppingResourceName);
    Assert.assertEquals(
        droppedIdealState.getInstanceStateMap(droppingPartitionName).get(droppingFromInstance),
        "DROPPED");
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testNonCompatibleConfiguration() throws IOException, HelixRebalanceException {
    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(), _algorithm);

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    String nonCompatibleResourceName = _resourceNames.get(0);
    clusterData.getIdealState(nonCompatibleResourceName)
        .setRebalancerClassName(CrushRebalanceStrategy.class.getName());
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    // The output shall not contains the nonCompatibleResource.
    resourceMap.remove(nonCompatibleResourceName);
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
  }

  // TODO test with invalid capacity configuration which will fail the cluster model constructing.
  @Test(dependsOnMethods = "testRebalance")
  public void testInvalidClusterStatus() throws IOException {
    WagedRebalancer rebalancer = new WagedRebalancer(new MockAssignmentMetadataStore(), _algorithm);

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    String invalidResource = _resourceNames.get(0);
    // The state model does not exist
    clusterData.getIdealState(invalidResource).setStateModelDefRef("foobar");
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().keySet().stream().collect(
        Collectors.toMap(resourceName -> resourceName, resourceName -> new Resource(resourceName)));
    try {
      rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.INVALID_CLUSTER_STATUS);
      Assert.assertEquals(ex.getMessage(),
          "Failed to generate cluster model. Failure Type: INVALID_CLUSTER_STATUS");
    }
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testInvalidRebalancerStatus() throws IOException {
    // Mock a metadata store that will fail on all the calls.
    AssignmentMetadataStore metadataStore = Mockito.mock(AssignmentMetadataStore.class);
    when(metadataStore.getBaseline())
        .thenThrow(new RuntimeException("Mock Error. Metadata store fails."));
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm);

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().keySet().stream().collect(
        Collectors.toMap(resourceName -> resourceName, resourceName -> new Resource(resourceName)));
    try {
      rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(),
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS);
      Assert.assertEquals(ex.getMessage(),
          "Failed to get the persisted assignment records. Failure Type: INVALID_REBALANCER_STATUS");
    }
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testAlgorithmExepction() throws IOException, HelixRebalanceException {
    RebalanceAlgorithm badAlgorithm = Mockito.mock(RebalanceAlgorithm.class);
    when(badAlgorithm.calculate(any())).thenThrow(new HelixRebalanceException("Algorithm fails.",
        HelixRebalanceException.Type.FAILED_TO_CALCULATE));

    WagedRebalancer rebalancer =
        new WagedRebalancer(new MockAssignmentMetadataStore(), badAlgorithm);

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().keySet().stream().collect(
        Collectors.toMap(resourceName -> resourceName, resourceName -> new Resource(resourceName)));
    try {
      rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(ex.getMessage(), "Algorithm fails. Failure Type: FAILED_TO_CALCULATE");
    }
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testRebalanceOnChanges() throws IOException, HelixRebalanceException {
    // Test continuously rebalance with the same rebalancer with different internal state. Ensure
    // that the rebalancer handles different input (different cluster changes) based on the internal
    // state in a correct way.

    // Note that this test relies on the MockRebalanceAlgorithm implementation. The mock algorithm
    // won't propagate any existing assignment from the cluster model.

    // Init mock metadatastore for the unit test
    MockAssignmentMetadataStore metadataStore = new MockAssignmentMetadataStore();
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm);

    // 1. rebalance with baseline calculation done
    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // Cluster config change will trigger baseline to be recalculated.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(entry -> entry.getKey(), entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().stream()
              .forEach(partition -> resource.addPartition(partition));
          return resource;
        }));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    // Since there is no special condition, the calculated IdealStates should be exactly the same
    // as the mock algorithm result.
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
    Map<String, ResourceAssignment> baseline = metadataStore.getBaseline();
    Assert.assertEquals(baseline, algorithmResult);
    Map<String, ResourceAssignment> bestPossibleAssignment =
        metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, algorithmResult);

    // 2. rebalance with one ideal state changed only
    String changedResourceName = _resourceNames.get(0);
    // Create a new cluster data cache to simulate cluster change
    clusterData = setupClusterDataCache();
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.IDEAL_STATE));
    IdealState is = clusterData.getIdealState(changedResourceName);
    // Update the tag so the ideal state will be marked as changed.
    is.setInstanceGroupTag("newTag");

    // Although the input contains 2 resources, the rebalancer shall only call the algorithm to
    // rebalance the changed one.
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> partialAlgorithmResult = _algorithm.getRebalanceResult();

    // Verify that only the changed resource has been included in the calculation.
    validateRebalanceResult(
        Collections.singletonMap(changedResourceName, new Resource(changedResourceName)),
        newIdealStates, partialAlgorithmResult);
    // Baseline should be empty, because there is no cluster topology change.
    baseline = metadataStore.getBaseline();
    Assert.assertEquals(baseline, Collections.emptyMap());
    // Best possible assignment contains the new assignment of only one resource.
    bestPossibleAssignment = metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, partialAlgorithmResult);

    // * Before the next test, recover the best possible assignment record.
    metadataStore.persistBestPossibleAssignment(algorithmResult);

    // 3. rebalance with current state change only
    // Create a new cluster data cache to simulate cluster change
    clusterData = setupClusterDataCache();
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CURRENT_STATE));
    // Modify any current state
    CurrentState cs =
        clusterData.getCurrentState(_testInstanceId, _sessionId).get(_resourceNames.get(0));
    // Update the tag so the ideal state will be marked as changed.
    cs.setInfo(_partitionNames.get(0), "mock update");

    // Although the input contains 2 resources, the rebalancer shall not try to recalculate
    // assignment since there is only current state change.
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    algorithmResult = _algorithm.getRebalanceResult();

    // Verify that only the changed resource has been included in the calculation.
    validateRebalanceResult(Collections.emptyMap(), newIdealStates, algorithmResult);
    // Both assignment state should be empty.
    baseline = metadataStore.getBaseline();
    Assert.assertEquals(baseline, Collections.emptyMap());
    bestPossibleAssignment = metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, Collections.emptyMap());

    // 4. rebalance with no change but best possible state record missing.
    // This usually happens when the persisted assignment state is gone.
    clusterData = setupClusterDataCache(); // Note this mock data cache won't report any change.
    // Even with no change, since the previous assignment is empty, the rebalancer will still
    // calculate the assignment for both resources.
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    algorithmResult = _algorithm.getRebalanceResult();
    // Verify that both resource has been included in the calculation.
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
    // Both assignment state should be empty since no cluster topology change.
    baseline = metadataStore.getBaseline();
    Assert.assertEquals(baseline, Collections.emptyMap());
    // The best possible assignment should be present.
    bestPossibleAssignment = metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, algorithmResult);
  }

  private void validateRebalanceResult(Map<String, Resource> resourceMap,
      Map<String, IdealState> newIdealStates, Map<String, ResourceAssignment> expectedResult) {
    Assert.assertEquals(newIdealStates.keySet(), resourceMap.keySet());
    for (String resourceName : expectedResult.keySet()) {
      Assert.assertTrue(newIdealStates.containsKey(resourceName));
      IdealState is = newIdealStates.get(resourceName);
      ResourceAssignment assignment = expectedResult.get(resourceName);
      Assert.assertEquals(is.getPartitionSet(), new HashSet<>(
          assignment.getMappedPartitions().stream().map(partition -> partition.getPartitionName())
              .collect(Collectors.toSet())));
      for (String partitionName : is.getPartitionSet()) {
        Assert.assertEquals(is.getInstanceStateMap(partitionName),
            assignment.getReplicaMap(new Partition(partitionName)));
      }
    }
  }
}
