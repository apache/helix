package org.apache.helix.controller.rebalancer.waged;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.monitoring.metrics.model.LatencyMetric;


/**
 * A manager class for fetching assignment from metadata store.
 */
class AssignmentManager {
  private final LatencyMetric _stateReadLatency;

  public AssignmentManager(LatencyMetric stateReadLatency) {
    _stateReadLatency = stateReadLatency;
  }

  /**
   * @param assignmentMetadataStore
   * @param currentStateOutput
   * @param resources
   * @return The current baseline assignment. If record does not exist in the
   *         assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  public Map<String, ResourceAssignment> getBaselineAssignment(AssignmentMetadataStore assignmentMetadataStore,
      CurrentStateOutput currentStateOutput, Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBaseline = new HashMap<>();
    if (assignmentMetadataStore != null) {
      try {
        _stateReadLatency.startMeasuringLatency();
        currentBaseline = assignmentMetadataStore.getBaseline();
        _stateReadLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current baseline assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    currentBaseline.keySet().retainAll(resources);

    // For resources without baseline, fall back to current state  assignments
    Set<String> missingResources = new HashSet<>(resources);
    missingResources.removeAll(currentBaseline.keySet());
    currentBaseline.putAll(currentStateOutput.getAssignment(missingResources));

    return currentBaseline;
  }

  /**
   * @param assignmentMetadataStore
   * @param currentStateOutput
   * @param resources
   * @return The current best possible assignment. If record does not exist in the
   *         assignmentMetadataStore, return the current state assignment.
   * @throws HelixRebalanceException
   */
  public Map<String, ResourceAssignment> getBestPossibleAssignment(
      AssignmentMetadataStore assignmentMetadataStore, CurrentStateOutput currentStateOutput,
      Set<String> resources) throws HelixRebalanceException {
    Map<String, ResourceAssignment> currentBestAssignment = new HashMap<>();
    if (assignmentMetadataStore != null) {
      try {
        _stateReadLatency.startMeasuringLatency();
        currentBestAssignment = assignmentMetadataStore.getBestPossibleAssignment();
        ;
        _stateReadLatency.endMeasuringLatency();
      } catch (Exception ex) {
        throw new HelixRebalanceException(
            "Failed to get the current best possible assignment because of unexpected error.",
            HelixRebalanceException.Type.INVALID_REBALANCER_STATUS, ex);
      }
    }
    currentBestAssignment.keySet().retainAll(resources);

    // For resources without best possible states, fall back to current state  assignments
    Set<String> missingResources = new HashSet<>(resources);
    missingResources.removeAll(currentBestAssignment.keySet());
    currentBestAssignment.putAll(currentStateOutput.getAssignment(missingResources));

    return currentBestAssignment;
  }
}
