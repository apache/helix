package org.apache.helix.integration.rebalancer;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCustomRebalancer {

  /**
   * This test was written because there is an edge case where an instance becomes disabled while a
   * partition is bootstrapping by way of pending
   * messages.
   * The newly bootstrapped partitions never get further state transitions because the instance
   * won't ever get added to instanceStateMap (this issue has been fixed). In other words, if there
   * are mapping changes while a partition is bootstrapping, the final state should go into the best
   * possible mapping for clusters to converge correctly.
   */
  @Test
  public void testDisabledBootstrappingPartitions() {
    String resourceName = "Test";
    String partitionName = "Test0";
    String instanceName = "localhost";
    String stateModelName = "OnlineOffline";
    StateModelDefinition stateModelDef = new OnlineOfflineSMD();

    IdealState idealState = new IdealState(resourceName);
    idealState.setStateModelDefRef(stateModelName);
    idealState.setPartitionState(partitionName, instanceName, "ONLINE");

    Resource resource = new Resource(resourceName);
    resource.addPartition(partitionName);

    CustomRebalancer customRebalancer = new CustomRebalancer();
    ResourceControllerDataProvider cache = mock(ResourceControllerDataProvider.class);
    when(cache.getStateModelDef(stateModelName)).thenReturn(stateModelDef);
    when(cache.getDisabledInstancesForPartition(resource.getResourceName(), partitionName))
        .thenReturn(ImmutableSet.of(instanceName));
    when(cache.getLiveInstances())
        .thenReturn(ImmutableMap.of(instanceName, new LiveInstance(instanceName)));

    CurrentStateOutput currOutput = new CurrentStateOutput();
    ResourceAssignment resourceAssignment =
        customRebalancer.computeBestPossiblePartitionState(cache, idealState, resource, currOutput);

    Assert.assertEquals(
        resourceAssignment.getReplicaMap(new Partition(partitionName)).get(instanceName),
        stateModelDef.getInitialState());
  }
}
