package org.apache.helix.controller.rebalancer;

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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAutoRebalanceStrategyImbalanceAssignment {
  private static final String resourceName = "ImbalanceResource";

  @Test
  public void testImbalanceAssignments() {
    final int nReplicas = 5;
    final int nPartitions = 20;
    final int nNode = 10;

    // Test all the combination of partitions, replicas and nodes
    for (int i = nPartitions; i > 0; i--) {
      for (int j = nReplicas; j > 0; j--) {
        for (int k = nNode; k > 0; k--) {
          if (k >= j) {
            testAssignment(i, j, k);
          }
        }
      }
    }
  }

  private void testAssignment(int nPartitions, int nReplicas, int nNode) {
    final List<String> instanceNames = new ArrayList<>();
    for (int i = 0; i < nNode; i++) {
      instanceNames.add("localhost_" + i);
    }
    List<String> partitions = new ArrayList<>(nPartitions);
    for (int i = 0; i < nPartitions; i++) {
      partitions.add(Integer.toString(i));
    }

    LinkedHashMap<String, Integer> states = new LinkedHashMap<>(2);
    states.put("OFFLINE", 0);
    states.put("ONLINE", nReplicas);

    AutoRebalanceStrategy strategy = new AutoRebalanceStrategy(resourceName, partitions, states);
    ZNRecord record = strategy.computePartitionAssignment(instanceNames, instanceNames,
        new HashMap<String, Map<String, String>>(0), new ClusterDataCache());

    for (Map<String, String> stateMapping : record.getMapFields().values()) {
      Assert.assertEquals(stateMapping.size(), nReplicas);
    }
  }
}
