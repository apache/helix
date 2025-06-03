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

public class InstanceCountImbalanceAlgorithm implements VirtualGroupImbalanceDetectionAlgorithm {
  private static final Logger LOG = Logger.getLogger(InstanceCountImbalanceAlgorithm.class.getName());
  private static final InstanceCountImbalanceAlgorithm _instance = new InstanceCountImbalanceAlgorithm();

  private InstanceCountImbalanceAlgorithm() { }

  public static InstanceCountImbalanceAlgorithm getInstance() {
    return _instance;
  }

  @Override
  public boolean isAssignmentImbalanced(int imbalanceThreshold,
      Map<String, Set<String>> virtualGroupToInstancesAssignment) {
    // Check if the assignment is imbalanced based on the threshold
    if (imbalanceThreshold < 0) {
      return false; // No imbalance check needed
    }
    int minInstances = Integer.MAX_VALUE;
    int maxInstances = Integer.MIN_VALUE;
    for (Set<String> instances : virtualGroupToInstancesAssignment.values()) {
      int size = instances.size();
      minInstances = Math.min(minInstances, size);
      maxInstances = Math.max(maxInstances, size);
    }

    if (maxInstances - minInstances > imbalanceThreshold) {
      LOG.info("Imbalance detected: maxInstances = " + maxInstances + ", minInstances = " + minInstances +
          ", threshold = " + imbalanceThreshold);
      return true; // Imbalance detected
    }
    return false;
  }
}
