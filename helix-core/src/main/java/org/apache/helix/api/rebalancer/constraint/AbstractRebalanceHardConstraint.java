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

public abstract class AbstractRebalanceHardConstraint {
  /**
   * Return a list of validate results.
   * @param resource Target resource
   * @param proposedAssignment Map of <PartitionName, lists of possible ParticipantName>
   * @return True in the return lists, if the proposed assignment does not valiate the constraint
   */
  public abstract Map<String, boolean[]> isValid(String resource,
      Map<String, String[]> proposedAssignment);

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
