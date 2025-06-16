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

/**
 * This algorithm calculates the imbalance score based on the MAX and MIN number of instances
 * assigned to each virtual group.
 */
public class InstanceCountImbalanceAlgorithm implements VirtualGroupImbalanceDetectionAlgorithm {
  private static final Logger LOG = Logger.getLogger(InstanceCountImbalanceAlgorithm.class.getName());
  private static final InstanceCountImbalanceAlgorithm _instance = new InstanceCountImbalanceAlgorithm();

  private InstanceCountImbalanceAlgorithm() { }

  public static InstanceCountImbalanceAlgorithm getInstance() {
    return _instance;
  }

  @Override
  public int getImbalanceScore(Map<String, Set<String>> virtualGroupToInstancesAssignment) {
    // Calculate the imbalance score based on the difference between max and min instance counts
    int minInstances = Integer.MAX_VALUE;
    int maxInstances = Integer.MIN_VALUE;
    String virtualGroupIdWithMaxInstances = null;
    String virtualGroupIdWithMinInstances = null;

    if (virtualGroupToInstancesAssignment == null || virtualGroupToInstancesAssignment.isEmpty()) {
      LOG.warning("Virtual group to instances assignment is empty or null. Returning 0 as imbalance score.");
      return 0; // No imbalance if there are no groups or instances
    }

    // Iterate through the assignment to find min and max instance counts
    // across all virtual groups
   for (Map.Entry<String, Set<String>> entry : virtualGroupToInstancesAssignment.entrySet()) {
      String virtualGroupId = entry.getKey();
      Set<String> instances = entry.getValue();
      int instanceCount = instances.size();

      if (instanceCount < minInstances) {
        minInstances = instanceCount;
        virtualGroupIdWithMinInstances = virtualGroupId;
      }
      if (instanceCount > maxInstances) {
        maxInstances = instanceCount;
        virtualGroupIdWithMaxInstances = virtualGroupId;
      }
    }
    // Log the virtual groups with min and max instances
    LOG.info("Virtual group with max instances: " + virtualGroupIdWithMaxInstances + " (" + maxInstances + ")"
        + ", Virtual group with min instances: " + virtualGroupIdWithMinInstances + " (" + minInstances + ")");

    return maxInstances - minInstances; // Return the imbalance score
  }
}
