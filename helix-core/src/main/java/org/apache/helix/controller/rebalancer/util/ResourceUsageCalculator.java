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

import org.apache.helix.api.rebalancer.constraint.dataprovider.PartitionWeightProvider;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;


public class ResourceUsageCalculator {
  /**
   * A convenient tool for calculating partition capacity usage based on the assignment and resource
   * weight provider.
   *
   * @param resourceAssignment
   * @param weightProvider
   * @return
   */
  public static Map<String, Integer> getResourceUsage(ResourcesStateMap resourceAssignment,
      PartitionWeightProvider weightProvider) {
    Map<String, Integer> newParticipantUsage = new HashMap<>();
    for (String resource : resourceAssignment.resourceSet()) {
      Map<Partition, Map<String, String>> stateMap =
          resourceAssignment.getPartitionStateMap(resource).getStateMap();
      for (Partition partition : stateMap.keySet()) {
        for (String participant : stateMap.get(partition).keySet()) {
          if (!newParticipantUsage.containsKey(participant)) {
            newParticipantUsage.put(participant, 0);
          }
          newParticipantUsage.put(participant, newParticipantUsage.get(participant) + weightProvider
              .getPartitionWeight(resource, partition.getPartitionName()));
        }
      }
    }
    return newParticipantUsage;
  }

  /**
   * Measure baseline divergence between baseline assignment and best possible assignment at
   * replica level. Example as below:
   * baseline =
   * {
   *    resource1={
   *       partition1={
   *          instance1=master,
   *          instance2=slave
   *       },
   *       partition2={
   *          instance2=slave
   *       }
   *    }
   * }
   * bestPossible =
   * {
   *    resource1={
   *       partition1={
   *          instance1=master,  <--- matched
   *          instance3=slave    <--- doesn't match
   *       },
   *       partition2={
   *          instance3=master   <--- doesn't match
   *       }
   *    }
   * }
   * baseline divergence = (matched: 1) / (doesn't match: 3) = 1/3 ~= 0.333
   * @param baseline baseline assignment
   * @param bestPossibleAssignment best possible assignment
   * @return double value range at [0.0, 1.0]
   */
  public static double measureBaselineDivergence(Map<String, ResourceAssignment> baseline,
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    int numMatchedReplicas = 0;
    int numTotalBestPossibleReplicas = 0;

    // 1. Check resource assignment names.
    for (Map.Entry<String, ResourceAssignment> resourceEntry : bestPossibleAssignment.entrySet()) {
      String resourceKey = resourceEntry.getKey();
      if (!baseline.containsKey(resourceKey)) {
        continue;
      }

      // Resource assignment names are matched.
      // 2. check partitions.
      Map<String, Map<String, String>> bestPossiblePartitions =
          resourceEntry.getValue().getRecord().getMapFields();
      Map<String, Map<String, String>> baselinePartitions =
          baseline.get(resourceKey).getRecord().getMapFields();

      for (Map.Entry<String, Map<String, String>> partitionEntry
          : bestPossiblePartitions.entrySet()) {
        String partitionName = partitionEntry.getKey();
        if (!baselinePartitions.containsKey(partitionName)) {
          continue;
        }

        // Partition names are matched.
        // 3. Check replicas.
        Map<String, String> bestPossibleReplicas = partitionEntry.getValue();
        Map<String, String> baselineReplicas = baselinePartitions.get(partitionName);

        for (Map.Entry<String, String> replicaEntry : bestPossibleReplicas.entrySet()) {
          String replicaName = replicaEntry.getKey();
          if (!baselineReplicas.containsKey(replicaName)) {
            continue;
          }

          // Replica names are matched.
          // 4. Check replica values.
          String bestPossibleReplica = replicaEntry.getValue();
          String baselineReplica = baselineReplicas.get(replicaName);
          if (bestPossibleReplica.equals(baselineReplica)) {
            numMatchedReplicas++;
          }
        }

        // Count total best possible replicas.
        numTotalBestPossibleReplicas += bestPossibleReplicas.size();
      }
    }

    return (double) numMatchedReplicas / (double) numTotalBestPossibleReplicas;
  }
}
