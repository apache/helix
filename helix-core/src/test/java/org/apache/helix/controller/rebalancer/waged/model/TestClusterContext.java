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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestClusterContext extends AbstractTestClusterModel {
  @BeforeClass
  public void initialize() {
    super.initialize();
  }

  @Test
  public void testNormalUsage() throws IOException {
    // Test 1 - initialize the cluster context based on the data cache.
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);

    ClusterContext context =
        new ClusterContext(assignmentSet, generateNodes(testCache), new HashMap<>(),
            new HashMap<>());

    Assert.assertEquals(context.getEstimatedMaxPartitionCount(), 4);
    Assert.assertEquals(context.getEstimatedMaxTopStateCount(), 2);
    Assert.assertEquals(context.getAssignmentForFaultZoneMap(), Collections.emptyMap());
    for (String resourceName : _resourceNames) {
      Assert.assertEquals(context.getEstimatedMaxPartitionByResource(resourceName), 2);
      Assert.assertEquals(
          context.getPartitionsForResourceAndFaultZone(_testFaultZoneId, resourceName),
          Collections.emptySet());
    }

    // Assign
    Map<String, Map<String, Set<String>>> expectedFaultZoneMap = Collections
        .singletonMap(_testFaultZoneId, assignmentSet.stream().collect(Collectors
            .groupingBy(AssignableReplica::getResourceName,
                Collectors.mapping(AssignableReplica::getPartitionName, Collectors.toSet()))));

    assignmentSet.stream().forEach(replica -> context
        .addPartitionToFaultZone(_testFaultZoneId, replica.getResourceName(),
            replica.getPartitionName()));
    Assert.assertEquals(context.getAssignmentForFaultZoneMap(), expectedFaultZoneMap);
    // Capacity with "item1" key is the highest utilized. Among 4 partitions, their weights are
    // 3, 5, 3, 5, so a total of 16/20 is used; the 2 master partitions have 3, 5, so 8/20 used.
    Assert.assertEquals(context.getEstimatedMaxUtilization(), 16.0 / 20.0, 0.005);
    Assert.assertEquals(context.getEstimatedTopStateMaxUtilization(), 8.0 / 20.0, 0.005);

    // release
    expectedFaultZoneMap.get(_testFaultZoneId).get(_resourceNames.get(0))
        .remove(_partitionNames.get(0));
    Assert.assertTrue(context.removePartitionFromFaultZone(_testFaultZoneId, _resourceNames.get(0),
        _partitionNames.get(0)));

    Assert.assertEquals(context.getAssignmentForFaultZoneMap(), expectedFaultZoneMap);
  }

  @Test(expectedExceptions = HelixException.class, expectedExceptionsMessageRegExp = "Resource Resource1 already has a replica from partition Partition1 in fault zone testZone")
  public void testDuplicateAssign() throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);
    ClusterContext context =
        new ClusterContext(assignmentSet, generateNodes(testCache), new HashMap<>(),
            new HashMap<>());
    context.addPartitionToFaultZone(_testFaultZoneId, _resourceNames.get(0), _partitionNames.get(0));
    // Insert again and trigger the error.
    context
        .addPartitionToFaultZone(_testFaultZoneId, _resourceNames.get(0), _partitionNames.get(0));
  }

  @DataProvider(name = "preferredScoringKeys")
  public static Object[][] preferredScoringKeys() {
    return new Object[][]{
            {Collections.singletonList("item1")},//valid key
            {Collections.singletonList("item3")},//valid key
            {Collections.singletonList("item-x")},//invalid key
            {null}
    };
  }

  @Test(dataProvider = "preferredScoringKeys")
  public void testEstimateMaxUtilization(List<String> preferredScoringKeys) throws IOException {
    ResourceControllerDataProvider testCache = setupClusterDataCache();
    Set<AssignableReplica> assignmentSet = generateReplicas(testCache);
    ClusterConfig clusterConfig = testCache.getClusterConfig();
    clusterConfig.setPreferredScoringKeys(preferredScoringKeys);
    ClusterContext context =
            new ClusterContext(assignmentSet, generateNodes(testCache), new HashMap<>(),
                    new HashMap<>(), clusterConfig);
    /**
     * Total Capacity and Total Usage values calculated from nodeSet and replicaSet above are as follows:
     * TotalCapacity : {"item1",20, "item2",40, "item3",30}
     * TotalUsage : {"item1",16, "item2",32, "item3",0}
     * Using these values to validate the results of estimateMaxUtilization.
     */

    validateResult(ImmutableMap.of("item1", 20, "item2", 40, "item3", 30),
            ImmutableMap.of("item1", 16, "item2", 32, "item3", 0),
            preferredScoringKeys, context.getEstimatedMaxUtilization());
  }

  private void validateResult(Map<String, Integer> totalCapacity, Map<String, Integer> totalUsage,
                              List<String> preferredScoringKeys, float actualEstimatedMaxUtilization) {
    if (preferredScoringKeys == null || preferredScoringKeys.size() == 0 || !totalCapacity.keySet().contains(preferredScoringKeys.get(0))) {
      //estimatedMaxUtilization calculated from all capacity keys
      Assert.assertEquals(actualEstimatedMaxUtilization, 0.8f);
      return;
    }
    //estimatedMaxUtilization calculated using preferredScoringKey only.
    Assert.assertEquals(actualEstimatedMaxUtilization, (float) totalUsage.get(preferredScoringKeys.get(0)) / totalCapacity.get(preferredScoringKeys.get(0)));
  }
}
