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
import java.util.List;
import java.util.Map;

import org.apache.helix.model.ResourceAssignment;

/**
 * The data model represents the optimal assignment of N replicas assigned to M instances;
 * It's mostly used as the return parameter of an assignment calculation algorithm; If the algorithm
 * failed to find optimal assignment given the endeavor, the user could check the failure reasons
 */
public class OptimalAssignment {
  private Map<AssignableNode, List<AssignableReplica>> _optimalAssignment = new HashMap<>();
  private Map<AssignableReplica, Map<AssignableNode, List<String>>> _failedAssignments =
      new HashMap<>();

  public OptimalAssignment() {

  }

  public void updateAssignments(ClusterModel clusterModel) {

  }

  // TODO: determine the output of final assignment format
  public Map<String, ResourceAssignment> getOptimalResourceAssignment() {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  // TODO: the convert method is not the best choice so far, will revisit the data model
  public OptimalAssignment convertFrom(ClusterModel clusterModel) {
    return this;
  }

  public void recordAssignmentFailure(AssignableReplica replica,
      Map<AssignableNode, List<String>> failedReasons) {
    _failedAssignments.put(replica, failedReasons);
  }

  public boolean hasAnyFailure() {
    return !_failedAssignments.isEmpty();
  }

  public String getFailures() {
    // TODO: format the error string
    return _failedAssignments.toString();
  }
}
