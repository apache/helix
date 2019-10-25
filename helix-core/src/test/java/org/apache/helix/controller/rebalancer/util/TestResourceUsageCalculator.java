package org.apache.helix.controller.rebalancer.util;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.util.TestInputLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestResourceUsageCalculator {
  @Test(dataProvider = "TestMeasureBaselineDivergenceInput")
  public void testMeasureBaselineDivergence(Map<String, Map<String, Map<String, String>>> baseline,
      Map<String, Map<String, Map<String, String>>> bestPossible) {
    Map<String, ResourceAssignment> baselineAssignment = buildResourceAssignment(baseline);
    Map<String, ResourceAssignment> bestPossibleAssignment = buildResourceAssignment(bestPossible);

    Assert.assertEquals(ResourceUsageCalculator
            .measureBaselineDivergence(baselineAssignment, bestPossibleAssignment),
        (double) 1 / (double) 3);
  }

  private Map<String, ResourceAssignment> buildResourceAssignment(
      Map<String, Map<String, Map<String, String>>> resourceMap) {
    Map<String, ResourceAssignment> assignment = new HashMap<>();
    for (Map.Entry<String, Map<String, Map<String, String>>> resourceEntry
        : resourceMap.entrySet()) {
      ResourceAssignment resource = new ResourceAssignment(resourceEntry.getKey());
      Map<String, Map<String, String>> partitionMap = resourceEntry.getValue();
      for (Map.Entry<String, Map<String, String>> partitionEntry : partitionMap.entrySet()) {
        resource.addReplicaMap(new Partition(partitionEntry.getKey()), partitionEntry.getValue());
      }

      assignment.put(resourceEntry.getKey(), resource);
    }

    return assignment;
  }

  @DataProvider(name = "TestMeasureBaselineDivergenceInput")
  public Object[][] loadTestMeasureBaselineDivergenceInput() {
    final String[] params = new String[]{"bestline", "bestPossible"};
    return TestInputLoader
        .loadTestInputs("TestResourceUsageCalculator.MeasureBaselineDivergence.json", params);
  }
}
