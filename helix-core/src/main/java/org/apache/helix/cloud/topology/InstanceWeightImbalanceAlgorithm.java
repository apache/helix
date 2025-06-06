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
  public boolean isAssignmentImbalanced(int imbalanceThreshold,
      Map<String, Set<String>> virtualGroupToInstancesAssignment) {
    // Check if the assignment is imbalanced based on the threshold

    if (imbalanceThreshold < 0) {
      LOG.info("Imbalance threshold is negative: " + imbalanceThreshold
          + ". No imbalance check needed.");
      return false; // No imbalance check needed
    }

    int minWeight = Integer.MAX_VALUE;
    int maxWeight = Integer.MIN_VALUE;

    for (Set<String> instances : virtualGroupToInstancesAssignment.values()) {
      int totalWeight = 0;
      for (String instance : instances) {
        InstanceConfig config = _configAccessor.getInstanceConfig(_clusterName, instance);
        int weight = config.getWeight();
        if (weight == WEIGHT_NOT_SET) {
          // If the weight is not set, we can't calculate the imbalance. Thereby, no imbalance
          // check needed
          LOG.warning(
              "Instance weight not provided. The result of imbalance algorithm is incorrect.");
          return false;
        }
        totalWeight += weight;
      }
      minWeight = Math.min(minWeight, totalWeight);
      maxWeight = Math.max(maxWeight, totalWeight);
    }

    if (maxWeight - minWeight > imbalanceThreshold) {
      LOG.info("Imbalance detected: maxWeight = " + maxWeight + ", minWeight = " + minWeight
          + ", threshold = " + imbalanceThreshold);
      return true; // Imbalance detected
    }
    return false; // No imbalance detected
  }
}
