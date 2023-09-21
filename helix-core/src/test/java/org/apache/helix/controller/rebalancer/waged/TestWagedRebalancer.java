package org.apache.helix.controller.rebalancer.waged;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.CrushRebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.constraints.MockRebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.AbstractTestClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.controller.rebalancer.waged.model.OptimalAssignment;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.monitoring.metrics.WagedRebalancerMetricCollector;
import org.apache.helix.monitoring.metrics.model.CountMetric;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestWagedRebalancer extends AbstractTestClusterModel {
  private MockRebalanceAlgorithm _algorithm;
  private MockAssignmentMetadataStore _metadataStore;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _algorithm = new MockRebalanceAlgorithm();

    // Initialize a mock assignment metadata store
    _metadataStore = new MockAssignmentMetadataStore();
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
      _partitionNames
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
      when(testCache.getAllInstances()).thenReturn(_instances);
    }

    return testCache;
  }

  @Test
  public void testRebalance() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Mocking the change types for triggering a baseline rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));

    when(clusterData.checkAndReduceCapacity(Mockito.any(), Mockito.any(),
        Mockito.any())).thenReturn(true);

    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    // Since there is no special condition, the calculated IdealStates should be exactly the same
    // as the mock algorithm result.
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);

    Assert.assertFalse(_metadataStore.getBaseline().isEmpty());
    Assert.assertFalse(_metadataStore.getBestPossibleAssignment().isEmpty());
    // Calculate with empty resource list. The rebalancer shall clean up all the assignment status.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.IDEAL_STATE));
    clusterData.getIdealStates().clear();
    newIdealStates = rebalancer
        .computeNewIdealStates(clusterData, Collections.emptyMap(), new CurrentStateOutput());
    Assert.assertTrue(newIdealStates.isEmpty());
    Assert.assertTrue(_metadataStore.getBaseline().isEmpty());
    Assert.assertTrue(_metadataStore.getBestPossibleAssignment().isEmpty());
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testPartialRebalance() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Mocking the change types for triggering a baseline rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));

    // Test with partial resources listed in the resourceMap input.
    // Remove the first resource from the input. Note it still exists in the cluster data cache.
    _metadataStore.reset();
    resourceMap.remove(_resourceNames.get(0));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testRebalanceWithCurrentState() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Mocking the change types for triggering a baseline rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));

    // Test with current state exists, so the rebalancer should calculate for the intermediate state
    // Create current state based on the cluster data cache.
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (String instanceName : _instances) {
      for (Map.Entry<String, CurrentState> csEntry : clusterData
          .getCurrentState(instanceName, _sessionId).entrySet()) {
        String resourceName = csEntry.getKey();
        CurrentState cs = csEntry.getValue();
        for (Map.Entry<String, String> partitionStateEntry : cs.getPartitionStateMap().entrySet()) {
          currentStateOutput.setCurrentState(resourceName,
              new Partition(partitionStateEntry.getKey()), instanceName,
              partitionStateEntry.getValue());
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
  public void testPartialBaselineAvailability() throws IOException, HelixRebalanceException {
    Map<String, ResourceAssignment> testResourceAssignmentMap = new HashMap<>();
    ZNRecord mappingNode = new ZNRecord(_resourceNames.get(0));
    HashMap<String, String> mapping = new HashMap<>();
    mapping.put(_testInstanceId, "MASTER");
    mappingNode.setMapField(_partitionNames.get(0), mapping);
    testResourceAssignmentMap.put(_resourceNames.get(0), new ResourceAssignment(mappingNode));

    _metadataStore.reset();
    _metadataStore.persistBaseline(testResourceAssignmentMap);
    _metadataStore.persistBestPossibleAssignment(testResourceAssignmentMap);

    // Test algorithm that passes along the best possible assignment
    RebalanceAlgorithm algorithm = Mockito.mock(RebalanceAlgorithm.class);
    when(algorithm.calculate(any())).thenAnswer(new Answer<OptimalAssignment>() {
      @Override
      public OptimalAssignment answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        ClusterModel argClusterModel = (ClusterModel) args[0];

        OptimalAssignment optimalAssignment = Mockito.mock(OptimalAssignment.class);
        when(optimalAssignment.getOptimalResourceAssignment())
            .thenReturn(argClusterModel.getContext().getBestPossibleAssignment());
        new OptimalAssignment();
        return optimalAssignment;
      }
    });

    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, algorithm, Optional.empty());

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Mocking the change types for triggering a baseline rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));

    // Mock a current state
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(_resourceNames.get(1), new Partition(_partitionNames.get(1)),
        _testInstanceId, "SLAVE");
    // Record current states into testBaseline; the logic should have loaded current state into
    // baseline as a fallback mechanism
    ZNRecord mappingNode2 = new ZNRecord(_resourceNames.get(1));
    HashMap<String, String> mapping2 = new HashMap<>();
    mapping2.put(_testInstanceId, "SLAVE");
    mappingNode2.setMapField(_partitionNames.get(1), mapping2);
    testResourceAssignmentMap.put(_resourceNames.get(1), new ResourceAssignment(mappingNode2));

    // Call compute, calculate() should have been called twice in global and partial rebalance
    rebalancer.computeNewIdealStates(clusterData, resourceMap, currentStateOutput);
    ArgumentCaptor<ClusterModel> argumentCaptor = ArgumentCaptor.forClass(ClusterModel.class);
    verify(algorithm, times(2)).calculate(argumentCaptor.capture());

    // In the first execution, the past baseline is loaded into the new best possible state
    Map<String, ResourceAssignment> firstCallBestPossibleAssignment =
        argumentCaptor.getAllValues().get(0).getContext().getBestPossibleAssignment();
    Assert.assertEquals(firstCallBestPossibleAssignment.size(), testResourceAssignmentMap.size());
    Assert.assertEquals(firstCallBestPossibleAssignment, testResourceAssignmentMap);
    // In the second execution, the result from the algorithm (which is just the best possible
    // state) is loaded as the baseline, and the best possible state is from persisted + current state
    Map<String, ResourceAssignment> secondCallBaselineAssignment =
        argumentCaptor.getAllValues().get(1).getContext().getBaselineAssignment();
    Map<String, ResourceAssignment> secondCallBestPossibleAssignment =
        argumentCaptor.getAllValues().get(1).getContext().getBestPossibleAssignment();
    Assert.assertEquals(secondCallBaselineAssignment.size(), testResourceAssignmentMap.size());
    Assert.assertEquals(secondCallBaselineAssignment, testResourceAssignmentMap);
    Assert.assertEquals(secondCallBestPossibleAssignment.size(), testResourceAssignmentMap.size());
    Assert.assertEquals(secondCallBestPossibleAssignment, testResourceAssignmentMap);

    Assert.assertEquals(_metadataStore.getBaseline().size(), testResourceAssignmentMap.size());
    Assert.assertEquals(_metadataStore.getBestPossibleAssignment().size(),
        testResourceAssignmentMap.size());
    Assert.assertEquals(_metadataStore.getBaseline(), testResourceAssignmentMap);
    Assert.assertEquals(_metadataStore.getBestPossibleAssignment(), testResourceAssignmentMap);
  }

  @Test(dependsOnMethods = "testRebalance", expectedExceptions = HelixRebalanceException.class, expectedExceptionsMessageRegExp = "Input contains invalid resource\\(s\\) that cannot be rebalanced by the WAGED rebalancer. \\[Resource1\\] Failure Type: INVALID_INPUT")
  public void testNonCompatibleConfiguration()
      throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    String nonCompatibleResourceName = _resourceNames.get(0);
    clusterData.getIdealState(nonCompatibleResourceName)
        .setRebalancerClassName(CrushRebalanceStrategy.class.getName());
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
  }

  // TODO test with invalid capacity configuration which will fail the cluster model constructing.
  @Test(dependsOnMethods = "testRebalance")
  public void testInvalidClusterStatus() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    String invalidResource = _resourceNames.get(0);
    // The state model does not exist
    clusterData.getIdealState(invalidResource).setStateModelDefRef("foobar");
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().keySet().stream().collect(
        Collectors.toMap(resourceName -> resourceName, Resource::new));
    try {
      rebalancer.computeBestPossibleAssignment(clusterData, resourceMap,
          clusterData.getEnabledLiveInstances(), new CurrentStateOutput(), _algorithm);
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(ex.getMessage(),
          "Failed to calculate for the new best possible. Failure Type: FAILED_TO_CALCULATE");
    }

    // The rebalance will be done with empty mapping result since there is no previously calculated
    // assignment.
    Assert.assertTrue(
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput())
            .isEmpty());
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testInvalidRebalancerStatus() throws IOException {
    // Mock a metadata store that will fail on all the calls.
    AssignmentMetadataStore metadataStore = Mockito.mock(AssignmentMetadataStore.class);
    when(metadataStore.getBestPossibleAssignment())
        .thenThrow(new RuntimeException("Mock Error. Metadata store fails."));
    WagedRebalancer rebalancer = new WagedRebalancer(metadataStore, _algorithm, Optional.empty());

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // The input resource Map shall contain all the valid resources.
    Map<String, Resource> resourceMap = clusterData.getIdealStates().keySet().stream().collect(
        Collectors.toMap(resourceName -> resourceName, Resource::new));
    try {
      rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(),
          HelixRebalanceException.Type.INVALID_REBALANCER_STATUS);
      Assert.assertEquals(ex.getMessage(),
          "Failed to get the current best possible assignment because of unexpected error. Failure Type: INVALID_REBALANCER_STATUS");
    }
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testAlgorithmException()
      throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Rebalance with normal configuration. So the assignment will be persisted in the metadata store.
    Map<String, IdealState> result =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());

    // Recreate a rebalance with the same metadata store but bad algorithm instance.
    RebalanceAlgorithm badAlgorithm = Mockito.mock(RebalanceAlgorithm.class);
    when(badAlgorithm.calculate(any())).thenThrow(new HelixRebalanceException("Algorithm fails.",
        HelixRebalanceException.Type.FAILED_TO_CALCULATE));
    rebalancer = new WagedRebalancer(_metadataStore, badAlgorithm, Optional.empty());

    // Calculation will fail
    try {
      rebalancer.computeBestPossibleAssignment(clusterData, resourceMap,
          clusterData.getEnabledLiveInstances(), new CurrentStateOutput(), badAlgorithm);
      Assert.fail("Rebalance shall fail.");
    } catch (HelixRebalanceException ex) {
      Assert.assertEquals(ex.getFailureType(), HelixRebalanceException.Type.FAILED_TO_CALCULATE);
      Assert.assertEquals(ex.getMessage(), "Failed to calculate for the new best possible. Failure Type: FAILED_TO_CALCULATE");
    }
    // But if call with the public method computeNewIdealStates(), the rebalance will return with
    // the previous rebalance result.
    Map<String, IdealState> newResult =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Assert.assertEquals(newResult, result);
    // Ensure failure has been recorded
    Assert.assertEquals(rebalancer.getMetricCollector().getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceFailureCounter.name(),
        CountMetric.class).getValue().longValue(), 1L);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testRebalanceOnChanges() throws IOException, HelixRebalanceException {
    // Test continuously rebalance with the same rebalancer with different internal state. Ensure
    // that the rebalancer handles different input (different cluster changes) based on the internal
    // state in a correct way.

    // Note that this test relies on the MockRebalanceAlgorithm implementation. The mock algorithm
    // won't propagate any existing assignment from the cluster model.
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    // 1. rebalance with baseline calculation done
    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // Cluster config change will trigger baseline to be recalculated.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    // Update the config so the cluster config will be marked as changed.
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    Map<String, Integer> defaultCapacityMap =
        new HashMap<>(clusterConfig.getDefaultInstanceCapacityMap());
    defaultCapacityMap.put("foobar", 0);
    clusterConfig.setDefaultInstanceCapacityMap(defaultCapacityMap);
    clusterData.setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));

    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    // Since there is no special condition, the calculated IdealStates should be exactly the same
    // as the mock algorithm result.
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
    Map<String, ResourceAssignment> baseline = _metadataStore.getBaseline();
    Assert.assertEquals(algorithmResult, baseline);
    Map<String, ResourceAssignment> bestPossibleAssignment =
        _metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(algorithmResult, bestPossibleAssignment);

    // 2. rebalance with one resource changed in the Resource Config znode only
    String changedResourceName = _resourceNames.get(0);
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.RESOURCE_CONFIG));
    ResourceConfig config = new ResourceConfig(clusterData.getResourceConfig(changedResourceName).getRecord());
    // Update the config so the resource will be marked as changed.
    Map<String, Map<String, Integer>> capacityMap = config.getPartitionCapacityMap();
    capacityMap.get(ResourceConfig.DEFAULT_PARTITION_KEY).put("foobar", 0);
    config.setPartitionCapacityMap(capacityMap);

    when(clusterData.getResourceConfig(changedResourceName)).thenReturn(config);
    clusterData.getResourceConfigMap().put(changedResourceName, config);

    // Although the input contains 2 resources, the rebalancer shall only call the algorithm to
    // rebalance the changed one.
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> partialAlgorithmResult = _algorithm.getRebalanceResult();

    // Verify that only the changed resource has been included in the calculation.
    validateRebalanceResult(
        Collections.singletonMap(changedResourceName, new Resource(changedResourceName)),
        newIdealStates, partialAlgorithmResult);
    // Best possible assignment contains the new assignment of only one resource.
    baseline = _metadataStore.getBaseline();
    Assert.assertEquals(baseline, partialAlgorithmResult);
    // Best possible assignment contains the new assignment of only one resource.
    bestPossibleAssignment = _metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, partialAlgorithmResult);

    // * Before the next test, recover the best possible assignment record.
    _metadataStore.persistBestPossibleAssignment(algorithmResult);
    _metadataStore.persistBaseline(algorithmResult);

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
    Map<String, ResourceAssignment> newAlgorithmResult = _algorithm.getRebalanceResult();

    // Verify that only the changed resource has been included in the calculation.
    validateRebalanceResult(Collections.emptyMap(), newIdealStates, newAlgorithmResult);
    // There should be no changes in the baseline since only the currentStates changed
    baseline = _metadataStore.getBaseline();
    Assert.assertEquals(baseline, algorithmResult);
    // The BestPossible assignment should have been updated since computeNewIdealStates() should have been called.
    bestPossibleAssignment = _metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, newAlgorithmResult);

    // 4. rebalance with no change but best possible state record missing.
    // This usually happens when the persisted assignment state is gone.
    clusterData = setupClusterDataCache(); // Note this mock data cache won't report any change.
    // Even with no change, since the previous assignment is empty, the rebalancer will still
    // calculate the assignment for both resources.
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    newAlgorithmResult = _algorithm.getRebalanceResult();
    // Verify that both resource has been included in the calculation.
    validateRebalanceResult(resourceMap, newIdealStates, newAlgorithmResult);
    // There should not be any changes in the baseline.
    baseline = _metadataStore.getBaseline();
    Assert.assertEquals(baseline, algorithmResult);
    // The BestPossible assignment should have been updated since computeNewIdealStates() should have been called.
    bestPossibleAssignment = _metadataStore.getBestPossibleAssignment();
    Assert.assertEquals(bestPossibleAssignment, newAlgorithmResult);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testEmergencyRebalance() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    MockRebalanceAlgorithm spyAlgorithm = Mockito.spy(new MockRebalanceAlgorithm());
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, spyAlgorithm, Optional.empty());

    // Cluster config change will trigger baseline to be recalculated.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Map<String, Resource> resourceMap =
        clusterData.getIdealStates().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Populate best possible assignment
    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    // Global Rebalance once, Partial Rebalance once
    verify(spyAlgorithm, times(2)).calculate(any());

    // Artificially insert an offline node in the best possible assignment
    Map<String, ResourceAssignment> bestPossibleAssignment =
        _metadataStore.getBestPossibleAssignment();
    String offlineResource = _resourceNames.get(0);
    String offlinePartition = _partitionNames.get(0);
    String offlineState = "MASTER";
    String offlineInstance = "offlineInstance";
    for (Partition partition : bestPossibleAssignment.get(offlineResource).getMappedPartitions()) {
      if (partition.getPartitionName().equals(offlinePartition)) {
        bestPossibleAssignment.get(offlineResource)
            .addReplicaMap(partition, Collections.singletonMap(offlineInstance, offlineState));
      }
    }
    _metadataStore.persistBestPossibleAssignment(bestPossibleAssignment);

    // This should trigger both emergency rebalance and partial rebalance
    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    ArgumentCaptor<ClusterModel> capturedClusterModel = ArgumentCaptor.forClass(ClusterModel.class);
    // 2 from previous case, Emergency + Partial from this case, 4 in total
    verify(spyAlgorithm, times(4)).calculate(capturedClusterModel.capture());
    // In the cluster model for Emergency rebalance, the assignableReplica is the offline one
    ClusterModel clusterModelForEmergencyRebalance = capturedClusterModel.getAllValues().get(2);
    Assert.assertEquals(clusterModelForEmergencyRebalance.getAssignableReplicaMap().size(), 1);
    Assert.assertEquals(clusterModelForEmergencyRebalance.getAssignableReplicaMap().get(offlineResource).size(), 1);
    AssignableReplica assignableReplica =
        clusterModelForEmergencyRebalance.getAssignableReplicaMap().get(offlineResource).iterator().next();
    Assert.assertEquals(assignableReplica.getPartitionName(), offlinePartition);
    Assert.assertEquals(assignableReplica.getReplicaState(), offlineState);

    bestPossibleAssignment = _metadataStore.getBestPossibleAssignment();
    for (Map.Entry<String, ResourceAssignment> entry : bestPossibleAssignment.entrySet()) {
      ResourceAssignment resourceAssignment = entry.getValue();
      for (Partition partition : resourceAssignment.getMappedPartitions()) {
        for (String instance: resourceAssignment.getReplicaMap(partition).keySet()) {
          Assert.assertNotSame(instance, offlineInstance);
        }
      }
    }
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testRebalanceOverwriteTrigger() throws IOException, HelixRebalanceException {
    _metadataStore.reset();

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // Enable delay rebalance
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1);
    clusterData.setClusterConfig(clusterConfig);

    // force create a fake offlineInstance that's in delay window
    Set<String> instances = new HashSet<>(_instances);
    String offlineInstance = "offlineInstance";
    instances.add(offlineInstance);
    when(clusterData.getAllInstances()).thenReturn(instances);
    Map<String, Long> instanceOfflineTimeMap = new HashMap<>();
    instanceOfflineTimeMap.put(offlineInstance, System.currentTimeMillis() + Integer.MAX_VALUE);
    when(clusterData.getInstanceOfflineTimeMap()).thenReturn(instanceOfflineTimeMap);
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getInstanceConfigMap();
    instanceConfigMap.put(offlineInstance, createMockInstanceConfig(offlineInstance));
    when(clusterData.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    // Set minActiveReplica to 0 so that requireRebalanceOverwrite returns false
    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState idealState = clusterData.getIdealState(resource);
      idealState.setMinActiveReplicas(0);
      isMap.put(resource, idealState);
    }
    when(clusterData.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(clusterData.getIdealStates()).thenReturn(isMap);

    MockRebalanceAlgorithm spyAlgorithm = Mockito.spy(new MockRebalanceAlgorithm());
    WagedRebalancer rebalancer = Mockito.spy(new WagedRebalancer(_metadataStore, spyAlgorithm, Optional.empty()));

    // Cluster config change will trigger baseline to be recalculated.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Map<String, Resource> resourceMap =
        clusterData.getIdealStates().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Populate best possible assignment
    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    verify(rebalancer, times(1)).requireRebalanceOverwrite(any(), any());
    Assert.assertEquals(rebalancer.getMetricCollector().getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteCounter.name(),
        CountMetric.class).getValue().longValue(), 0L);
    Assert.assertEquals(rebalancer.getMetricCollector().getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteLatencyGauge.name(),
        LatencyMetric.class).getLastEmittedMetricValue().longValue(), -1L);

    // Set minActiveReplica to 1 so that requireRebalanceOverwrite returns true
    for (String resource : _resourceNames) {
      IdealState idealState = clusterData.getIdealState(resource);
      idealState.setMinActiveReplicas(3);
      isMap.put(resource, idealState);
    }
    when(clusterData.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(clusterData.getIdealStates()).thenReturn(isMap);

    _metadataStore.reset();
    // Update the config so the cluster config will be marked as changed.
    clusterConfig = clusterData.getClusterConfig();
    Map<String, Integer> defaultCapacityMap =
        new HashMap<>(clusterConfig.getDefaultInstanceCapacityMap());
    defaultCapacityMap.put("foobar", 0);
    clusterConfig.setDefaultInstanceCapacityMap(defaultCapacityMap);
    clusterData.setClusterConfig(clusterConfig);
    rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    verify(rebalancer, times(2)).requireRebalanceOverwrite(any(), any());
    Assert.assertEquals(rebalancer.getMetricCollector().getMetric(
        WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteCounter.name(),
        CountMetric.class).getValue().longValue(), 1L);
    Assert.assertTrue(rebalancer.getMetricCollector()
        .getMetric(WagedRebalancerMetricCollector.WagedRebalancerMetricNames.RebalanceOverwriteLatencyGauge.name(),
            LatencyMetric.class).getLastEmittedMetricValue() > 0L);
  }

  @Test(dependsOnMethods = "testRebalanceOverwriteTrigger")
  public void testRebalanceOverwrite() throws HelixRebalanceException, IOException {
    _metadataStore.reset();

    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    // Enable delay rebalance
    ClusterConfig clusterConfig = clusterData.getClusterConfig();
    clusterConfig.setDelayRebalaceEnabled(true);
    clusterConfig.setRebalanceDelayTime(1);
    clusterData.setClusterConfig(clusterConfig);

    String instance0 = _testInstanceId;
    String instance1 = instance0  + "1";
    String instance2 = instance0 + "2";
    String offlineInstance = "offlineInstance";

    // force create a fake offlineInstance that's in delay window
    Set<String> instances = new HashSet<>(_instances);
    instances.add(offlineInstance);
    when(clusterData.getAllInstances()).thenReturn(instances);
    when(clusterData.getEnabledInstances()).thenReturn(instances);
    when(clusterData.getEnabledLiveInstances()).thenReturn(ImmutableSet.of(instance0, instance1, instance2));
    Map<String, Long> instanceOfflineTimeMap = new HashMap<>();
    instanceOfflineTimeMap.put(offlineInstance, System.currentTimeMillis() + Integer.MAX_VALUE);
    when(clusterData.getInstanceOfflineTimeMap()).thenReturn(instanceOfflineTimeMap);
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getInstanceConfigMap();
    instanceConfigMap.put(offlineInstance, createMockInstanceConfig(offlineInstance));
    when(clusterData.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState idealState = clusterData.getIdealState(resource);
      idealState.setMinActiveReplicas(2);
      isMap.put(resource, idealState);
    }
    when(clusterData.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(clusterData.getIdealStates()).thenReturn(isMap);

    MockRebalanceAlgorithm algorithm = new MockRebalanceAlgorithm();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, algorithm, Optional.empty());

    // Cluster config change will trigger baseline to be recalculated.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Map<String, Resource> resourceMap =
        clusterData.getIdealStates().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));

    Map<String, Map<String, Map<String, String>>> input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of(instance1, "MASTER", instance2, "SLAVE"),
            _partitionNames.get(1), ImmutableMap.of(instance2, "MASTER", offlineInstance, "OFFLINE"), // Partition2-SLAVE
            _partitionNames.get(2), ImmutableMap.of(instance1, "SLAVE", instance2, "MASTER"),
            _partitionNames.get(3), ImmutableMap.of(instance1, "SLAVE", instance2, "SLAVE")),
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of(instance1, "MASTER", instance2, "SLAVE"),
            _partitionNames.get(1), ImmutableMap.of(instance1, "MASTER", instance2, "SLAVE"),
            _partitionNames.get(2), ImmutableMap.of(instance1, "MASTER", instance2, "SLAVE"),
            _partitionNames.get(3), ImmutableMap.of(offlineInstance, "OFFLINE", instance2, "SLAVE")) // Partition4-MASTER
    );
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    input.forEach((resource, inputMap) ->
        inputMap.forEach((partition, stateInstance) ->
            stateInstance.forEach((tmpInstance, state) ->
                currentStateOutput.setCurrentState(resource, new Partition(partition), tmpInstance, state))));
    rebalancer.setPartialRebalanceAsyncMode(true);
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, currentStateOutput);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(0)).getPreferenceLists().size(), 4);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(1)).getPreferenceLists().size(), 4);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(0)).getPreferenceList(_partitionNames.get(1)).size(), 3);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(0)).getPreferenceList(_partitionNames.get(3)).size(), 2);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(1)).getPreferenceList(_partitionNames.get(3)).size(), 3);
    Assert.assertEquals(newIdealStates.get(_resourceNames.get(1)).getPreferenceList(_partitionNames.get(0)).size(), 2);
  }

  @Test(dependsOnMethods = "testRebalance")
  public void testReset() throws IOException, HelixRebalanceException {
    _metadataStore.reset();
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());
    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    // Mocking the change types for triggering a baseline rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Map<String, IdealState> newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    Map<String, ResourceAssignment> algorithmResult = _algorithm.getRebalanceResult();
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);

    // Clean up algorithm result for the next test step
    algorithmResult.clear();
    // Try to trigger a new rebalancer, since nothing has been changed. There will be no rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    Assert.assertEquals(
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput()),
        newIdealStates);
    algorithmResult = _algorithm.getRebalanceResult();
    Assert.assertEquals(algorithmResult, Collections.emptyMap());

    // Reset the rebalance and do the same operation. Without any cache info, the rebalancer will
    // finish the complete rebalance.
    rebalancer.reset();
    algorithmResult.clear();
    // Try to trigger a new rebalancer, since nothing has been changed. There will be no rebalance.
    when(clusterData.getRefreshedChangeTypes())
        .thenReturn(Collections.singleton(HelixConstants.ChangeType.CLUSTER_CONFIG));
    newIdealStates =
        rebalancer.computeNewIdealStates(clusterData, resourceMap, new CurrentStateOutput());
    algorithmResult = _algorithm.getRebalanceResult();
    validateRebalanceResult(resourceMap, newIdealStates, algorithmResult);
  }

  private void validateRebalanceResult(Map<String, Resource> resourceMap,
      Map<String, IdealState> newIdealStates, Map<String, ResourceAssignment> expectedResult) {
    Assert.assertEquals(newIdealStates.keySet(), resourceMap.keySet());
    for (String resourceName : expectedResult.keySet()) {
      Assert.assertTrue(newIdealStates.containsKey(resourceName));
      IdealState is = newIdealStates.get(resourceName);
      ResourceAssignment assignment = expectedResult.get(resourceName);
      Assert.assertEquals(is.getPartitionSet(), new HashSet<>(assignment.getMappedPartitions()
          .stream().map(Partition::getPartitionName).collect(Collectors.toSet())));
      for (String partitionName : is.getPartitionSet()) {
        Assert.assertEquals(is.getInstanceStateMap(partitionName),
            assignment.getReplicaMap(new Partition(partitionName)));
      }
    }
  }

  @Test
  public void testResourceWeightProvider() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    WagedResourceWeightsProvider dataProvider = new WagedResourceWeightsProvider(testCache);
    Map<String, Integer> weights1 = ImmutableMap.of("item1", 3, "item2", 6, "item3", 0);
    Assert.assertEquals(dataProvider.getPartitionWeights("Resource1", "Partition1"), weights1);
    Assert.assertEquals(dataProvider.getPartitionWeights("Resource1", "Partition2"), weights1);
    Map<String, Integer> weights2 = ImmutableMap.of("item1", 5, "item2", 10, "item3", 0);
    Assert.assertEquals(dataProvider.getPartitionWeights("Resource2", "Partition2"), weights2);
  }

  @Test
  public void testInstanceCapacityProvider() throws IOException, HelixRebalanceException {
    WagedRebalancer rebalancer = new WagedRebalancer(_metadataStore, _algorithm, Optional.empty());

    // Generate the input for the rebalancer.
    ResourceControllerDataProvider clusterData = setupClusterDataCache();

    // force create a fake offlineInstance that's in delay window
    Set<String> instances = new HashSet<>(_instances);
    when(clusterData.getAllInstances()).thenReturn(instances);
    when(clusterData.getEnabledInstances()).thenReturn(instances);
    when(clusterData.getEnabledLiveInstances()).thenReturn(instances);
    Map<String, InstanceConfig> instanceConfigMap = clusterData.getInstanceConfigMap();
    when(clusterData.getInstanceConfigMap()).thenReturn(instanceConfigMap);

    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      IdealState idealState = clusterData.getIdealState(resource);
      idealState.setMinActiveReplicas(2);
      isMap.put(resource, idealState);
    }
    Map<String, Resource> resourceMap = clusterData.getIdealStates().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Resource resource = new Resource(entry.getKey());
          entry.getValue().getPartitionSet().forEach(resource::addPartition);
          return resource;
        }));
    WagedInstanceCapacity provider = new WagedInstanceCapacity(clusterData);

    Map<String, Integer> weights1 = ImmutableMap.of("item1", 20, "item2", 40, "item3", 30);
    Map<String, Integer> capacity = provider.getInstanceAvailableCapacity("testInstanceId");
    Assert.assertEquals(provider.getInstanceAvailableCapacity("testInstanceId"), weights1);
    Assert.assertEquals(provider.getInstanceAvailableCapacity("testInstanceId1"), weights1);
    Assert.assertEquals(provider.getInstanceAvailableCapacity("testInstanceId2"), weights1);
  }
}
