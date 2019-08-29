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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class TestClusterModelProvider extends AbstractTestClusterModel {
  Set<String> _instances;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _instances = new HashSet<>();
    _instances.add(_testInstanceId);
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
    }

    return testCache;
  }

  @Test
  public void testGenerateClusterModel() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    // 1. test generating a cluster model with empty assignment
    ClusterModel clusterModel = ClusterModelProvider.generateClusterModel(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    // There should be no existing assignment.
    Assert.assertFalse(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .anyMatch(resourceMap -> !resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getCurrentAssignmentCount() != 0));
    // Have all 3 instances
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), _instances);
    // Shall have 2 resources and 12 replicas
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 12));

    // 2. test with only one active node
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.singleton(_testInstanceId), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), Collections.singleton(_testInstanceId));
    // Shall have 4 assignable replicas because there is only one valid node.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 4));

    // 3. test with no active instance
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 0);
    // Shall have 0 assignable replicas because there is only n0 valid node.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.isEmpty()));

    // 4. test with best possible assignment
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

    // Generate a cluster model based on the best possible assignment
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.emptyMap(), Collections.emptyMap(), bestPossibleAssignment);
    // There should be 4 existing assignments in total (each resource has 2) in the specified instance
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().stream()
            .allMatch(partitionSet -> partitionSet.size() == 2)));
    Assert.assertEquals(
        clusterModel.getAssignableNodes().get(_testInstanceId).getCurrentAssignmentCount(), 4);
    // Since each resource has 2 replicas assigned, the assignable replica count should be 10.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 10));

    // 5. test with best possible assignment but cluster topology is changed
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.singletonMap(HelixConstants.ChangeType.CLUSTER_CONFIG,
            Collections.emptySet()), Collections.emptyMap(), bestPossibleAssignment);
    // There should be no existing assignment since the topology change invalidates all existing assignment
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getCurrentAssignmentCount() != 0));
    // Shall have 2 resources and 12 replicas
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 12));

    // 6. test with best possible assignment and one resource config change
    // Generate a cluster model based on the same best possible assignment, but resource1 config is changed
    String changedResourceName = _resourceNames.get(0);
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instances, Collections.singletonMap(HelixConstants.ChangeType.RESOURCE_CONFIG,
            Collections.singleton(changedResourceName)), Collections.emptyMap(),
        bestPossibleAssignment);
    // There should be no existing assignment for all the resource except for resource2.
    Assert.assertEquals(clusterModel.getContext().getAssignmentForFaultZoneMap().size(), 1);
    Map<String, Set<String>> resourceAssignmentMap =
        clusterModel.getContext().getAssignmentForFaultZoneMap().get(_testFaultZoneId);
    // Should be only resource2 in the map
    Assert.assertEquals(resourceAssignmentMap.size(), 1);
    for (String resource : _resourceNames) {
      Assert
          .assertEquals(resourceAssignmentMap.getOrDefault(resource, Collections.emptySet()).size(),
              resource.equals(changedResourceName) ? 0 : 2);
    }
    // Only the first instance will have 2 assignment from resource2.
    for (String instance : _instances) {
      Assert
          .assertEquals(clusterModel.getAssignableNodes().get(instance).getCurrentAssignmentCount(),
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
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        limitedActiveInstances, Collections.emptyMap(), Collections.emptyMap(),
        bestPossibleAssignment);
    // There should be no existing assignment.
    Assert.assertFalse(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .anyMatch(resourceMap -> !resourceMap.isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getCurrentAssignmentCount() != 0));
    // Have only 2 instances
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), limitedActiveInstances);
    // Since only 2 instances are active, we shall have 8 assignable replicas in each resource.
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 8));

  }
}
