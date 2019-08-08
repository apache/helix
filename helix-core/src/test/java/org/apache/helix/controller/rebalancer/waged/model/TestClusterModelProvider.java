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

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Resource;
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
  Set<String> _instanceList;

  @BeforeClass
  public void initialize() {
    super.initialize();
    _instanceList = new HashSet<>();
    _instanceList.add(_testInstanceId);
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
      _instanceList.add(instanceName);
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
    // 1. test generate a cluster model with empty assignment
    ClusterModel clusterModel = ClusterModelProvider.generateClusterModel(testCache,
        _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        _instanceList, Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    // There should be no existing assignment.
    Assert.assertTrue(clusterModel.getContext().getAssignmentForFaultZoneMap().values().stream()
        .allMatch(resourceMap -> resourceMap.values().isEmpty()));
    Assert.assertFalse(clusterModel.getAssignableNodes().values().stream()
        .anyMatch(node -> node.getCurrentAssignmentCount() != 0));
    // Have all 3 instances
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 3);
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), _instanceList);
    // Shall have 2 resources and 12 replicas
    Assert.assertEquals(clusterModel.getAssignableReplicaMap().keySet().size(), 2);
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 12));

    // 2. test with less active node
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.singleton(_testInstanceId), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 1);
    Assert.assertEquals(
        clusterModel.getAssignableNodes().values().stream().map(AssignableNode::getInstanceName)
            .collect(Collectors.toSet()), Collections.singleton(_testInstanceId));
    // Shall have 4 assignable replicas because there is only one valid node.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 4));

    // 3. test with none active instance
    clusterModel = ClusterModelProvider.generateClusterModel(testCache, _resourceNames.stream()
            .collect(Collectors.toMap(resource -> resource, resource -> new Resource(resource))),
        Collections.emptySet(), Collections.emptyMap(), Collections.emptyMap(),
        Collections.emptyMap());
    // Have only one instance
    Assert.assertEquals(clusterModel.getAssignableNodes().size(), 0);
    // Shall have 0 assignable replicas because there is only n0 valid node.
    Assert.assertTrue(clusterModel.getAssignableReplicaMap().values().stream()
        .allMatch(replicaSet -> replicaSet.size() == 0));

    // 4. test with best possible assignment

    // 5. test with best possible assignment but cluster topology change

    // 6. test with best possible assignment and one resource config change

    // 7. test with best possible assignment but the instance becomes inactive
  }
}
