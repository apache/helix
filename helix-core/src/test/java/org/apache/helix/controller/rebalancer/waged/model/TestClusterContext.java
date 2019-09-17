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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
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

    ClusterContext context = new ClusterContext(assignmentSet, 2, new HashMap<>(), new HashMap<>());

    // Note that we left some margin for the max estimation.
    Assert.assertEquals(context.getEstimatedMaxPartitionCount(), 3);
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
    ClusterContext context = new ClusterContext(assignmentSet, 2, new HashMap<>(), new HashMap<>());
    context
        .addPartitionToFaultZone(_testFaultZoneId, _resourceNames.get(0), _partitionNames.get(0));
    // Insert again and trigger the error.
    context
        .addPartitionToFaultZone(_testFaultZoneId, _resourceNames.get(0), _partitionNames.get(0));
  }
}
