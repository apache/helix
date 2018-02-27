package org.apache.helix.api.rebalancer.constraint;

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

import org.apache.helix.controller.common.ResourcesStateMap;

import java.util.Map;

public abstract class AbstractRebalanceSoftConstraint {
  private static int DEFAULT_WEIGHT = 1;
  protected int _weight = DEFAULT_WEIGHT;

  /**
   * Evaluate how the given assignment fits the constraint.
   * @param resource Target resource
   * @param proposedAssignment Map of <PartitionName, lists of possible ParticipantName>
   * @return Score about the assignment according to this constraint. Larger number means better fit under this constraint.
   */
  public abstract Map<String, int[]> evaluate(String resource,
      Map<String, String[]> proposedAssignment);

  /**
   * @return The soft constraint's weight that will be used to consolidate the final evaluation score.
   * Aggregated evaluation score = SUM(constraint_evaluation * weight).
   */
  public int getConstraintWeight() {
    return _weight;
  }

  /**
   * Update constraint status with the pending assignment.
   * @param pendingAssignment
   */
  public void updateAssignment(ResourcesStateMap pendingAssignment) {
    // By default, constraint won't need to understand assignment updates.
    // If the constraint calculation depends on current assignment,
    // a constraint implementation can choose to override this method and update any internal states.
  }
}
