package org.apache.helix.cloud.topology;

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

import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.model.InstanceConfig;

import static org.apache.helix.model.InstanceConfig.WEIGHT_NOT_SET;

/**
 * This class implements the VirtualGroupImbalanceDetectionAlgorithm interface to calculate
 * the imbalance score based on the MAX and MIN weights of instances assigned to virtual groups.
 */
public class InstanceWeightImbalanceAlgorithm implements VirtualGroupImbalanceDetectionAlgorithm {
  private static final Logger LOG =
      Logger.getLogger(InstanceWeightImbalanceAlgorithm.class.getName());
  private static final InstanceWeightImbalanceAlgorithm _instance =
      new InstanceWeightImbalanceAlgorithm();
  private static ConfigAccessor _configAccessor;
  private static String _clusterName;

  private InstanceWeightImbalanceAlgorithm() {
  }

  public static InstanceWeightImbalanceAlgorithm getInstance(ConfigAccessor accessor,
      String clusterName) {
    _configAccessor = accessor;
    _clusterName = clusterName;
    return _instance;
  }

  @Override
  public int getImbalanceScore(Map<String, Set<String>> virtualGroupToInstancesAssignment) {
    if (virtualGroupToInstancesAssignment == null || virtualGroupToInstancesAssignment.isEmpty()) {
      LOG.warning(
          "Virtual group to instances assignment is empty or null. Returning 0 as imbalance score"
              + ".");
      return 0; // No imbalance if there are no groups or instances
    }

    // Calculate the imbalance score based on the difference between max and min weights
    int minWeight = Integer.MAX_VALUE;
    int maxWeight = Integer.MIN_VALUE;
    String virtualGroupIdWithMaxWeight = null;
    String virtualGroupIdWithMinWeight = null;

    // Iterate through the assignment to find min and max weights across all virtual groups
    for (Map.Entry<String, Set<String>> entry : virtualGroupToInstancesAssignment.entrySet()) {
      String virtualGroupId = entry.getKey();
      Set<String> instances = entry.getValue();
      int totalWeight = 0;

      // Sum the weights of all instances in the virtual group
      for (String instanceId : instances) {
        InstanceConfig instanceConfig = _configAccessor.getInstanceConfig(_clusterName, instanceId);
        int weight = instanceConfig.getWeight();
        if (weight == WEIGHT_NOT_SET) {
          throw new IllegalStateException(
              "Instance weight is not set for instance: " + instanceId + " in cluster: "
                  + _clusterName);
        }
        totalWeight += weight;
      }

      if (totalWeight < minWeight) {
        minWeight = totalWeight;
        virtualGroupIdWithMinWeight = virtualGroupId;
      }
      if (totalWeight > maxWeight) {
        maxWeight = totalWeight;
        virtualGroupIdWithMaxWeight = virtualGroupId;
      }
    }

    // Log the virtual groups with min and max weights
    LOG.info(
        "Virtual group with max weight: " + virtualGroupIdWithMaxWeight + " (" + maxWeight + ")"
            + ", Virtual group with min weight: " + virtualGroupIdWithMinWeight + " (" + minWeight
            + ")");

    return maxWeight - minWeight; // Return the imbalance score
  }
}
