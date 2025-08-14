package org.apache.helix.controller.rebalancer.waged.model;

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
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.util.DelayedRebalanceUtil;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TestClusterModelProvider extends AbstractTestClusterModel {
  Map<String, ResourceConfig> _resourceConfigMap = new HashMap<>();

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

    // Set up 2 more instances
    for (int i = 1; i < 3; i++) {
      String instanceName = _testInstanceId + i;
      _instances.add(instanceName);
      // 1. Set up the default instance information with capacity configuration.
      InstanceConfig testInstanceConfig = createMockInstanceConfig(instanceName);
      Map<String, InstanceConfig> instanceConfigMap = testCache.getAssignableInstanceConfigMap();
      instanceConfigMap.put(instanceName, testInstanceConfig);
      when(testCache.getAssignableInstanceConfigMap()).thenReturn(instanceConfigMap);
      when(testCache.getInstanceConfigMap()).thenReturn(instanceConfigMap);
      // 2. Mock the live instance node for the default instance.
      LiveInstance testLiveInstance = createMockLiveInstance(instanceName);
      Map<String, LiveInstance> liveInstanceMap = testCache.getAssignableLiveInstances();
      liveInstanceMap.put(instanceName, testLiveInstance);
      when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    }

    return testCache;
  }

  @Test
  public void testFindToBeAssignedReplicasForMinActiveReplica() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    String instance1 = _testInstanceId;
    String offlineInstance = _testInstanceId + "1";
    String instance2 = _testInstanceId + "2";
    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    liveInstanceMap.put(instance1, createMockLiveInstance(instance1));
    liveInstanceMap.put(instance2, createMockLiveInstance(instance2));
    Set<String> activeInstances = new HashSet<>();
    activeInstances.add(instance1);
    activeInstances.add(instance2);
    when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    when(testCache.getEnabledLiveInstances()).thenReturn(activeInstances);

    // test 0, empty input
    Assert.assertEquals(
        DelayedRebalanceUtil.findToBeAssignedReplicasForMinActiveReplica(testCache, Collections.emptySet(),
            activeInstances, Collections.emptyMap(), new HashMap<>()),
        Collections.emptySet());

    // test 1, one partition under minActiveReplica
    Map<String, Map<String, Map<String, String>>> input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(1), ImmutableMap.of("OFFLINE", offlineInstance)), // Partition2-MASTER
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(3), ImmutableMap.of("SLAVE", instance1))
    );
    Map<String, Set<AssignableReplica>> replicaMap = new HashMap<>(); // to populate
    Map<String, ResourceAssignment> currentAssignment = new HashMap<>(); // to populate
    prepareData(input, replicaMap, currentAssignment, testCache, 1);

    Map<String, Set<AssignableReplica>> allocatedReplicas = new HashMap<>();
    Set<AssignableReplica> toBeAssignedReplicas =
        DelayedRebalanceUtil.findToBeAssignedReplicasForMinActiveReplica(testCache, replicaMap.keySet(), activeInstances,
            currentAssignment, allocatedReplicas);

    Assert.assertEquals(toBeAssignedReplicas.size(), 1);
    Assert.assertTrue(toBeAssignedReplicas.stream().map(AssignableReplica::toString).collect(Collectors.toSet())
        .contains("Resource1-Partition2-MASTER"));
    AssignableReplica replica = toBeAssignedReplicas.iterator().next();
    Assert.assertEquals(replica.getReplicaState(), "MASTER");
    Assert.assertEquals(replica.getPartitionName(), "Partition2");

    // test 2, no additional replica to be assigned
    testCache = setupClusterDataCache();
    when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    when(testCache.getEnabledLiveInstances()).thenReturn(activeInstances);
    input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(1), ImmutableMap.of("SLAVE", instance2)),
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(3), ImmutableMap.of("SLAVE", instance2))
    );
    replicaMap = new HashMap<>(); // to populate
    currentAssignment = new HashMap<>(); // to populate
    prepareData(input, replicaMap, currentAssignment, testCache, 1);
    allocatedReplicas = new HashMap<>();
    toBeAssignedReplicas =
        DelayedRebalanceUtil.findToBeAssignedReplicasForMinActiveReplica(testCache, replicaMap.keySet(), activeInstances,
            currentAssignment, allocatedReplicas);
    Assert.assertTrue(toBeAssignedReplicas.isEmpty());
    Assert.assertEquals(allocatedReplicas.get(instance1).size(), 2);
    Assert.assertEquals(allocatedReplicas.get(instance2).size(), 2);

    // test 3, minActiveReplica==2, two partitions falling short
    testCache = setupClusterDataCache();
    when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    when(testCache.getEnabledLiveInstances()).thenReturn(activeInstances);
    input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(1), ImmutableMap.of("MASTER", instance1, "OFFLINE", offlineInstance)), // Partition2-SLAVE
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(3), ImmutableMap.of("SLAVE", instance1, "OFFLINE", offlineInstance)) // Partition4-MASTER
    );
    replicaMap = new HashMap<>(); // to populate
    currentAssignment = new HashMap<>(); // to populate
    prepareData(input, replicaMap, currentAssignment, testCache, 2);
    allocatedReplicas = new HashMap<>();
    toBeAssignedReplicas =
        DelayedRebalanceUtil.findToBeAssignedReplicasForMinActiveReplica(testCache, replicaMap.keySet(), activeInstances,
            currentAssignment, allocatedReplicas);
    Assert.assertEquals(toBeAssignedReplicas.size(), 2);
    Assert.assertEquals(toBeAssignedReplicas.stream().map(AssignableReplica::toString).collect(Collectors.toSet()),
        ImmutableSet.of("Resource1-Partition2-SLAVE", "Resource2-Partition4-MASTER"));
    Assert.assertEquals(allocatedReplicas.get(instance1).size(), 4);
    Assert.assertEquals(allocatedReplicas.get(instance2).size(), 2);
  }

  @Test(dependsOnMethods = "testFindToBeAssignedReplicasForMinActiveReplica")
  public void testClusterModelForDelayedRebalanceOverwrite() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    String instance1 = _testInstanceId;
    String offlineInstance = _testInstanceId + "1";
    String instance2 = _testInstanceId + "2";
    Map<String, LiveInstance> liveInstanceMap = new HashMap<>();
    liveInstanceMap.put(instance1, createMockLiveInstance(instance1));
    liveInstanceMap.put(instance2, createMockLiveInstance(instance2));
    Set<String> activeInstances = new HashSet<>();
    activeInstances.add(instance1);
    activeInstances.add(instance2);
    when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    when(testCache.getEnabledLiveInstances()).thenReturn(activeInstances);

    // test 1, one partition under minActiveReplica
    Map<String, Map<String, Map<String, String>>> input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(1), ImmutableMap.of("OFFLINE", offlineInstance), // Partition2-MASTER
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance2),
            _partitionNames.get(3), ImmutableMap.of("MASTER", instance2)),
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance2),
            _partitionNames.get(1), ImmutableMap.of("MASTER", instance2),
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance1),
            _partitionNames.get(3), ImmutableMap.of("OFFLINE", offlineInstance)) // Partition4-MASTER
    );
    Map<String, Set<AssignableReplica>> replicaMap = new HashMap<>(); // to populate
    Map<String, ResourceAssignment> currentAssignment = new HashMap<>(); // to populate
    prepareData(input, replicaMap, currentAssignment, testCache, 1);

    Map<String, Resource> resourceMap = _resourceNames.stream().collect(Collectors.toMap(resource -> resource, Resource::new));
    ClusterModel clusterModel = ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(testCache,
        resourceMap, activeInstances, currentAssignment);
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableNodes().containsKey(instance1));
    Assert.assertTrue(clusterModel.getAssignableNodes().containsKey(instance2));
    Assert.assertEquals(clusterModel.getAssignableNodes().get(instance1).getAssignedReplicas().size(), 2);
    Assert.assertEquals(clusterModel.getAssignableNodes().get(instance2).getAssignedReplicas().size(), 4);

    Assert.assertEquals(clusterModel.getAssignableReplicaMap().get("Resource1").size(), 1);
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().get("Resource1").iterator().next().toString(),
        "Resource1-Partition2-MASTER");
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().get("Resource2").size(), 1);
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().get("Resource2").iterator().next().toString(),
        "Resource2-Partition4-MASTER");

    // test 2, minActiveReplica==2, three partitions falling short
    testCache = setupClusterDataCache();
    when(testCache.getAssignableLiveInstances()).thenReturn(liveInstanceMap);
    when(testCache.getEnabledLiveInstances()).thenReturn(activeInstances);
    input = ImmutableMap.of(
        _resourceNames.get(0),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(1), ImmutableMap.of("MASTER", instance1, "OFFLINE", offlineInstance), // Partition2-SLAVE
            _partitionNames.get(2), ImmutableMap.of("OFFLINE", offlineInstance, "SLAVE", instance2), // Partition3-MASTER
            _partitionNames.get(3), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2)),
        _resourceNames.get(1),
        ImmutableMap.of(
            _partitionNames.get(0), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(1), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(2), ImmutableMap.of("MASTER", instance1, "SLAVE", instance2),
            _partitionNames.get(3), ImmutableMap.of("OFFLINE", offlineInstance, "ERROR", instance2)) // Partition4-MASTER
    );
    replicaMap = new HashMap<>(); // to populate
    currentAssignment = new HashMap<>(); // to populate
    prepareData(input, replicaMap, currentAssignment, testCache, 2);
    clusterModel = ClusterModelProvider.generateClusterModelForDelayedRebalanceOverwrites(testCache,
        resourceMap, activeInstances, currentAssignment);
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableNodes().containsKey(instance1));
    Assert.assertTrue(clusterModel.getAssignableNodes().containsKey(instance2));
    Assert.assertEquals(clusterModel.getAssignableNodes().get(instance1).getAssignedReplicas().size(), 6);
    Assert.assertEquals(clusterModel.getAssignableNodes().get(instance2).getAssignedReplicas().size(), 7);

    Set<String> replicaSet = clusterModel.getAssignableReplicaMap().get(_resourceNames.get(0))
        .stream()
        .map(AssignableReplica::toString)
        .collect(Collectors.toSet());
    Assert.assertEquals(replicaSet.size(), 2);
    Assert.assertTrue(replicaSet.contains("Resource1-Partition2-SLAVE"));
    Assert.assertTrue(replicaSet.contains("Resource1-Partition3-MASTER"));
    replicaSet = clusterModel.getAssignableReplicaMap().get(_resourceNames.get(1))
        .stream()
        .map(AssignableReplica::toString)
        .collect(Collectors.toSet());
    Assert.assertEquals(replicaSet.size(), 1);
    Assert.assertTrue(replicaSet.contains("Resource2-Partition4-MASTER"));
  }

  /**
   * Prepare mock objects with given input. This methods prepare replicaMap and populate testCache with currentState.
   *
   * @param input <resource, <partition, <state, instance> > >
   * @param replicaMap The data map to prepare, a set of AssignableReplica by resource name.
   * @param currentAssignment The data map to prepare, resourceAssignment by resource name
   * @param testCache The mock object to prepare
   */
  private void prepareData(Map<String, Map<String, Map<String, String>>> input,
      Map<String, Set<AssignableReplica>> replicaMap,
      Map<String, ResourceAssignment> currentAssignment,
      ResourceControllerDataProvider testCache,
      int minActiveReplica) {

    // Set up mock idealstate
    Map<String, IdealState> isMap = new HashMap<>();
    for (String resource : _resourceNames) {
      ResourceConfig resourceConfig = new ResourceConfig.Builder(resource)
          .setMinActiveReplica(minActiveReplica)
          .setNumReplica(3)
          .build();
      _resourceConfigMap.put(resource, resourceConfig);
      IdealState is = new IdealState(resource);
      is.setNumPartitions(_partitionNames.size());
      is.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
      is.setStateModelDefRef("MasterSlave");
      is.setReplicas("3");
      is.setMinActiveReplicas(minActiveReplica);
      is.setRebalancerClassName(WagedRebalancer.class.getName());
      _partitionNames.forEach(partition -> is.setPreferenceList(partition, Collections.emptyList()));
      isMap.put(resource, is);
    }
    when(testCache.getIdealState(anyString())).thenAnswer(
        (Answer<IdealState>) invocationOnMock -> isMap.get(invocationOnMock.getArguments()[0]));
    when(testCache.getResourceConfig(anyString())).thenAnswer(
        (Answer<ResourceConfig>) invocationOnMock -> _resourceConfigMap.get(invocationOnMock.getArguments()[0]));


    // <instance, <resource, CurrentState>>
    Map<String, Map<String, CurrentState>> currentStateByInstanceByResource = new HashMap<>();
    Map<String, Map<String, Map<String, String>>> stateByInstanceByResourceByPartition = new HashMap<>();

    for (String resource : input.keySet()) {
      Set<AssignableReplica> replicas = new HashSet<>();
      replicaMap.put(resource, replicas);
      ResourceConfig resourceConfig = _resourceConfigMap.get(resource);
      for (String partition : input.get(resource).keySet()) {
        input.get(resource).get(partition).forEach(
            (state, instance) -> {
              stateByInstanceByResourceByPartition
                  .computeIfAbsent(instance, k -> new HashMap<>())
                  .computeIfAbsent(resource, k -> new HashMap<>())
                  .put(partition, state);
              replicas.add(new MockAssignableReplica(resourceConfig, partition, state));
            });
      }
    }
    for (String instance : stateByInstanceByResourceByPartition.keySet()) {
      for (String resource : stateByInstanceByResourceByPartition.get(instance).keySet()) {
        Map<String, String> partitionState = stateByInstanceByResourceByPartition.get(instance).get(resource);
        CurrentState testCurrentStateResource = mockCurrentStateResource(partitionState);
        currentStateByInstanceByResource.computeIfAbsent(instance, k -> new HashMap<>()).put(resource, testCurrentStateResource);
      }
    }

    for (String instance : currentStateByInstanceByResource.keySet()) {
      when(testCache.getCurrentState(instance, _sessionId)).thenReturn(currentStateByInstanceByResource.get(instance));
      when(testCache.getCurrentState(instance, _sessionId, false))
          .thenReturn(currentStateByInstanceByResource.get(instance));
    }

    // Mock a baseline assignment based on the current states.
    for (String resource : _resourceNames) {
      // <partition, <instance, state>>
      Map<String, Map<String, String>> assignmentMap = new HashMap<>();
      for (String instance : _instances) {
        CurrentState cs = testCache.getCurrentState(instance, _sessionId).get(resource);
        if (cs != null) {
          for (Map.Entry<String, String> stateEntry : cs.getPartitionStateMap().entrySet()) {
            assignmentMap.computeIfAbsent(stateEntry.getKey(), k -> new HashMap<>())
                .put(instance, stateEntry.getValue());
          }
          ResourceAssignment assignment = new ResourceAssignment(resource);
          assignmentMap.keySet().forEach(partition -> assignment
              .addReplicaMap(new Partition(partition), assignmentMap.get(partition)));
          currentAssignment.put(resource, assignment);
        }
      }
    }
  }

  private CurrentState mockCurrentStateResource(Map<String, String> partitionState) {
    CurrentState testCurrentStateResource = Mockito.mock(CurrentState.class);
    when(testCurrentStateResource.getResourceName()).thenReturn(_resourceNames.get(0));
    when(testCurrentStateResource.getPartitionStateMap()).thenReturn(partitionState);
    when(testCurrentStateResource.getStateModelDefRef()).thenReturn("MasterSlave");
    when(testCurrentStateResource.getSessionId()).thenReturn(_sessionId);
    for (Map.Entry<String, String> entry : partitionState.entrySet()) {
      when(testCurrentStateResource.getState(entry.getKey())).thenReturn(entry.getValue());
    }
    return testCurrentStateResource;
  }

  @Test
  public void testGenerateClusterModel() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    // 1. test generating a cluster model with empty assignment
    ClusterModel clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.emptyMap(), Collections.emptyMap());
    // There should be no existing assignment.
    Assert.assertFalse(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .anyMatch(resourceMap -> !resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getAssignedReplicaCount() != 0));
    // Have all 3 instances
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), _instances);
    // Shall have 2 resources and 4 replicas, since all nodes are in the same fault zone.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 4));

    // Adjust instance fault zone, so they have different fault zones.
    testCache.getAssignableInstanceConfigMap().values().stream()
        .forEach(config -> config.setZoneId(config.getInstanceName()));
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.emptyMap(), Collections.emptyMap());
    // Shall have 2 resources and 12 replicas after fault zone adjusted.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 12));

    // 2. test with only one active node
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.singleton(_testInstanceId), Collections.emptyMap(), Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), Collections.singleton(_testInstanceId));
    // Shall have 4 assignable replicas because there is only one valid node.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 4));

    // 3. test with no active instance
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 0);
    // Shall have 0 assignable replicas because there are 0 valid nodes.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.isEmpty()));

    // 4. test with baseline assignment
    // Mock a baseline assignment based on the current states.
    Map<String, ResourceAssignment> baselineAssignment = new HashMap<>();
    for (String resource : _resourceNames) {
      // <partition, <instance, state>>
      Map<String, Map<String, String>> assignmentMap = new HashMap<>();
      CurrentState cs = testCache.getCurrentState(_testInstanceId, _sessionId).get(resource);
      if (cs != null) {
        for (Map.Entry<String, String> stateEntry : cs.getPartitionStateMap().entrySet()) {
          assignmentMap.computeIfAbsent(stateEntry.getKey(), k -> new HashMap<>())
              .put(_testInstanceId, stateEntry.getValue());
        }
        ResourceAssignment assignment = new ResourceAssignment(resource);
        assignmentMap.keySet().stream().forEach(partition -> assignment
            .addReplicaMap(new Partition(partition), assignmentMap.get(partition)));
        baselineAssignment.put(resource, assignment);
      }
    }

    // Generate a cluster model based on the best possible assignment
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.emptyMap(), baselineAssignment);
    // There should be 4 existing assignments in total (each resource has 2) in the specified instance
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().stream()
            .allMatch(partitionSet -> partitionSet.size() == 2)));
    Assert.assertEquals(
        clusterModel.getAssignableNodes().get(_testInstanceId).getAssignedReplicaCount(), 4);
    // Since each resource has 2 replicas assigned, the assignable replica count should be 10.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 10));

    // 5. test with best possible assignment but cluster topology is changed
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances,
        Collections.singletonMap(HelixConstants.ChangeType.CLUSTER_CONFIG, Collections.emptySet()),
        baselineAssignment);
    // There should be no existing assignment since the topology change invalidates all existing assignment
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getAssignedReplicaCount() != 0));
    // Shall have 2 resources and 12 replicas
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 12));

    // 6. test with best possible assignment and one resource config change
    // Generate a cluster model based on the same best possible assignment, but resource1 config is changed
    String changedResourceName = _resourceNames.get(0);
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.singletonMap(HelixConstants.ChangeType.RESOURCE_CONFIG,
            Collections.singleton(changedResourceName)), baselineAssignment);
    // There should be no existing assignment for all the resource except for resource2
    Assert.assertEquals(clusterModel.getContext().getAssignmentForFaultZoneMap().size(), 1);
    Map<String, Set<String>> resourceAssignmentMap =
        clusterModel.getContext().getAssignmentForFaultZoneMap().get(_testInstanceId);
    // Should be only resource2 in the map
    Assert.assertEquals(resourceAssignmentMap.size(), 1);
    for (String resource : _resourceNames) {
      Assert
          .assertEquals(resourceAssignmentMap.getOrDefault(resource, Collections.emptySet()).size(),
              resource.equals(changedResourceName) ? 0 : 2);
    }
    // Only the first instance will have 2 assignment from resource2.
    for (String instance : _instances) {
      Assert.assertEquals(clusterModel.getAssignableNodes().get(instance).getAssignedReplicaCount(),
          instance.equals(_testInstanceId) ? 2 : 0);
    }
    // Shall have 2 resources and 12 replicas
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().keySet().size(), 2);
    for (String resource : _resourceNames) {
      Assert.assertEquals(clusterModel.getAssignableReplicaMap().get(resource).size(),
          resource.equals(changedResourceName) ? 12 : 10);
    }

    // 7. test with best possible assignment but the instance becomes inactive
    // Generate a cluster model based on the best possible assignment, but the assigned node is disabled
    Set<String> limitedActiveInstances = new HashSet<>(_instances);
    limitedActiveInstances.remove(_testInstanceId);
    clusterModel = ClusterModelProvider.generateClusterModelForBaseline(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        limitedActiveInstances, Collections.emptyMap(), baselineAssignment);
    // There should be no existing assignment.
    Assert.assertFalse(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .anyMatch(resourceMap -> !resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getAssignedReplicaCount() != 0));
    // Have only 2 instances
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), limitedActiveInstances);
    // Since only 2 instances are active, we shall have 8 assignable replicas in each resource.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 8));
  }

  @Test (dependsOnMethods = "testGenerateClusterModel")
  public void testGenerateClusterModelForPartialRebalance() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    // 1. test generating a cluster model with empty assignment
    ClusterModel clusterModel = ClusterModelProvider
        .generateClusterModelForPartialRebalance(testCache, _resourceNames.stream()
                .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
            _instances, Collections.emptyMap(), Collections.emptyMap());
    // There should be no existing assignment.
    Assert.assertFalse(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .anyMatch(resourceMap -> !resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getAssignedReplicaCount() != 0));
    // Have all 3 instances
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), _instances);
    // Shall have 0 resources and 0 replicas since the baseline is empty. The partial rebalance
    // should not rebalance any replica.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 0);

    // Adjust instance fault zone, so they have different fault zones.
    testCache.getAssignableInstanceConfigMap().values().stream()
        .forEach(config -> config.setZoneId(config.getInstanceName()));

    // 2. test with a pair of identical best possible assignment and baseline assignment
    // Mock a best possible assignment based on the current states.
    Map<String, ResourceAssignment> bestPossibleAssignment = new HashMap<>();
    for (String resource : _resourceNames) {
      // <partition, <instance, state>>
      Map<String, Map<String, String>> assignmentMap = new HashMap<>();
      CurrentState cs = testCache.getCurrentState(_testInstanceId, _sessionId).get(resource);
      if (cs != null) {
        for (Map.Entry<String, String> stateEntry : cs.getPartitionStateMap().entrySet()) {
          assignmentMap.computeIfAbsent(stateEntry.getKey(), k -> new HashMap<>())
              .put(_testInstanceId, stateEntry.getValue());
        }
        ResourceAssignment assignment = new ResourceAssignment(resource);
        assignmentMap.keySet().stream().forEach(partition -> assignment
            .addReplicaMap(new Partition(partition), assignmentMap.get(partition)));
        bestPossibleAssignment.put(resource, assignment);
      }
    }
    Map<String, ResourceAssignment> baseline = new HashMap<>(bestPossibleAssignment);
    // Generate a cluster model for partial rebalance
    clusterModel = ClusterModelProvider.generateClusterModelForPartialRebalance(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, baseline, bestPossibleAssignment);
    // There should be 4 existing assignments in total (each resource has 2) in the specified instance
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().stream()
            .allMatch(partitionSet -> partitionSet.size() == 2)));
    Assert.assertEquals(
        clusterModel.getAssignableNodes().get(_testInstanceId).getAssignedReplicaCount(), 4);
    // Since the best possible matches the baseline, no replica needs to be reassigned.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 0);

    // 3. test with inactive instance in the baseline and the best possible assignment
    Set<String> partialInstanceList = new HashSet<>(_instances);
    partialInstanceList.remove(_testInstanceId);
    clusterModel = ClusterModelProvider.generateClusterModelForPartialRebalance(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        partialInstanceList, baseline, bestPossibleAssignment);
    // Have the other 2 active instances
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 2);
    // All the replicas in the existing assignment should be rebalanced.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 2));
    // Shall have 0 assigned replicas
    Assert.assertTrue(clusterModel.getAssignableNodes().values().stream()
        .allMatch(assignableNode -> assignableNode.getAssignedReplicaCount() == 0));

    // 4. test with one resource that is only in the baseline
    String resourceInBaselineOnly = _resourceNames.get(0);
    Map<String, ResourceAssignment> partialBestPossibleAssignment =
        new HashMap<>(bestPossibleAssignment);
    partialBestPossibleAssignment.remove(resourceInBaselineOnly);
    // Generate a cluster mode with the adjusted best possible assignment
    clusterModel = ClusterModelProvider.generateClusterModelForPartialRebalance(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, baseline, partialBestPossibleAssignment);
    // There should be 2 existing assignments in total in the specified instance
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().stream()
            .allMatch(partitionSet -> partitionSet.size() == 2)));
    // Only the replicas of one resource require rebalance
    Assert.assertEquals(
        clusterModel.getAssignableNodes().get(_testInstanceId).getAssignedReplicaCount(), 2);
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 1);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().containsKey(resourceInBaselineOnly));
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 2));

    // 5. test with one resource only in the best possible assignment
    String resourceInBestPossibleOnly = _resourceNames.get(1);
    Map<String, ResourceAssignment> partialBaseline = new HashMap<>(baseline);
    partialBaseline.remove(resourceInBestPossibleOnly);
    // Generate a cluster model with the adjusted baseline
    clusterModel = ClusterModelProvider.generateClusterModelForPartialRebalance(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, partialBaseline, bestPossibleAssignment);
    // There should be 2 existing assignments in total and all of them require rebalance.
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().stream()
            .allMatch(partitionSet -> partitionSet.size() == 2)));
    Assert.assertEquals(
        clusterModel.getAssignableNodes().get(_testInstanceId).getAssignedReplicaCount(), 2);
    // No need to rebalance the replicas that are not in the baseline yet.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 0);
  }

  static class MockAssignableReplica extends AssignableReplica {
    MockAssignableReplica(ResourceConfig resourceConfig, String partition, String replicaState) {
      super(new ClusterConfig("testCluster"), resourceConfig, partition, replicaState, 1);
    }
  }

  @Test
  public void testGetAllAssignableNodes_success() {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");

    InstanceConfig instance1 = new InstanceConfig("instance1");
    instance1.setDomain("zone=zone1,instance=instance1");
    InstanceConfig instance2 = new InstanceConfig("instance2");
    instance2.setDomain("zone=zone2,instance=instance2");

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put("instance1", instance1);
    instanceConfigMap.put("instance2", instance2);

    Set<String> activeInstances = new HashSet<>(Arrays.asList("instance1", "instance2"));

    Set<AssignableNode> nodes = invokeGetAllAssignableNodes(clusterConfig, instanceConfigMap, activeInstances);
    Assert.assertEquals(nodes.size(), 2);
    Set<String> nodeNames = new HashSet<>();
    for (AssignableNode node : nodes) {
      nodeNames.add(node.getInstanceName());
    }
    Assert.assertTrue(nodeNames.contains("instance1"));
    Assert.assertTrue(nodeNames.contains("instance2"));
  }

  @Test
  public void testGetAllAssignableNodes_missingConfig() {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");

    InstanceConfig instance1 = new InstanceConfig("instance1");
    instance1.setDomain("zone=zone1,instance=instance1");
    // instance2 config missing
    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put("instance1", instance1);

    Set<String> activeInstances = new HashSet<>(Arrays.asList("instance1", "instance2"));

    Set<AssignableNode> nodes = invokeGetAllAssignableNodes(clusterConfig, instanceConfigMap, activeInstances);
    Assert.assertEquals(nodes.size(), 1);
    AssignableNode node = nodes.iterator().next();
    Assert.assertEquals(node.getInstanceName(), "instance1");
  }

  @Test
  public void testGetAllAssignableNodes_illegalArgumentException() {
    ClusterConfig clusterConfig = new ClusterConfig("testCluster");
    clusterConfig.setTopologyAwareEnabled(true);
    clusterConfig.setTopology("/zone/instance");
    clusterConfig.setFaultZoneType("zone");

    // Create an invalid InstanceConfig that will cause IllegalArgumentException
    InstanceConfig invalidInstance = Mockito.mock(InstanceConfig.class);
    Mockito.when(invalidInstance.getInstanceName()).thenReturn("invalidInstance");
    // Simulate invalid domain or missing required config
    Mockito.when(invalidInstance.getDomainAsMap()).thenThrow(new IllegalArgumentException("Invalid config"));

    Map<String, InstanceConfig> instanceConfigMap = new HashMap<>();
    instanceConfigMap.put("invalidInstance", invalidInstance);

    Set<String> activeInstances = new HashSet<>(Collections.singletonList("invalidInstance"));

    Set<AssignableNode> nodes = invokeGetAllAssignableNodes(clusterConfig, instanceConfigMap, activeInstances);
    // Should be empty due to exception
    Assert.assertTrue(nodes.isEmpty());
  }

  private Set<AssignableNode> invokeGetAllAssignableNodes(ClusterConfig clusterConfig,
      Map<String, InstanceConfig> instanceConfigMap, Set<String> activeInstances) {
    // Use reflection to access private static method
    try {
      Method getAllAssignableNodesMethod = ClusterModelProvider.class.getDeclaredMethod(
          "getAllAssignableNodes", ClusterConfig.class, Map.class, Set.class);
      getAllAssignableNodesMethod.setAccessible(true);
      Object result = getAllAssignableNodesMethod.invoke(null, clusterConfig, instanceConfigMap, activeInstances);
      return (Set<AssignableNode>) result;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
