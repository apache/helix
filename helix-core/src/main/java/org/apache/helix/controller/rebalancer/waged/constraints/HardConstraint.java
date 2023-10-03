package org.apache.helix.controller.rebalancer.waged.constraints;

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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;


/**
 * Evaluate a partition allocation proposal and return YES or NO based on the cluster context.
 * Any proposal fails one or more hard constraints will be rejected.
 */
abstract class HardConstraint {

  /**
   * Check if the replica could be assigned to the node
   * @return True if the proposed assignment is valid; False otherwise
   */
  abstract ValidationResult isAssignmentValid(AssignableNode node, AssignableReplica replica,
      ClusterContext clusterContext);


  /**
   * Stores the assignment validation result and the error message, in case of failure.
   */
  public static class ValidationResult {
    enum Status {
      SUCCESSFUL,
      FAILED;
    }

    private final Status _status;
    private final String _errorMessage;

    private ValidationResult(Status status, String errorMessage) {
      _status = status;
      _errorMessage = errorMessage;
    }

    public static ValidationResult ok() {
      return new ValidationResult(Status.SUCCESSFUL, null);
    }

    public static ValidationResult fail(String errorMessage) {
      return new ValidationResult(Status.FAILED, errorMessage);
    }

    public boolean isSuccessful() {
      return _status == Status.SUCCESSFUL;
    }

    public boolean iFailed() {
      return _status == Status.FAILED;
    }

    public String getErrorMessage() {
      return _errorMessage;
    }
  }

}
