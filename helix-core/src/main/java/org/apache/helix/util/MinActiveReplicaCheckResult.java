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

package org.apache.helix.util;

/**
 * Result container for minimum active replica validation checks.
 * Provides details about validation outcome and specific failure information.
 */
public class MinActiveReplicaCheckResult {
  private final boolean passed;
  private final String resourceName;
  private final String partitionName;
  private final int currentActiveReplicas;
  private final int requiredMinActiveReplicas;

  private MinActiveReplicaCheckResult(boolean passed, String resourceName, String partitionName,
      int currentActiveReplicas, int requiredMinActiveReplicas) {
    this.passed = passed;
    this.resourceName = resourceName;
    this.partitionName = partitionName;
    this.currentActiveReplicas = currentActiveReplicas;
    this.requiredMinActiveReplicas = requiredMinActiveReplicas;
  }

  public static MinActiveReplicaCheckResult passed() {
    return new MinActiveReplicaCheckResult(true, null, null, -1, -1);
  }

  public static MinActiveReplicaCheckResult failed(String resourceName, String partitionName,
      int currentActiveReplicas, int requiredMinActiveReplicas) {
    return new MinActiveReplicaCheckResult(false, resourceName, partitionName,
        currentActiveReplicas, requiredMinActiveReplicas);
  }

  public boolean isPassed() {
    return passed;
  }

  public String getResourceName() {
    return resourceName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public int getCurrentActiveReplicas() {
    return currentActiveReplicas;
  }

  public int getRequiredMinActiveReplicas() {
    return requiredMinActiveReplicas;
  }

  @Override
  public String toString() {
    if (passed) {
      return "MIN_ACTIVE_REPLICA_CHECK_FAILED: passed";
    } else {
      return String.format("MIN_ACTIVE_REPLICA_CHECK_FAILED: Resource %s partition %s has %d/%d active replicas",
          resourceName, partitionName, currentActiveReplicas, requiredMinActiveReplicas);
    }
  }
}