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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A placeholder before we have the implementation.
 * The constraint-based rebalance algorithm that is used in the WAGED rebalancer.
 */
public class ConstraintsRebalanceAlgorithm implements RebalanceAlgorithm {
  private static final Logger LOG = LoggerFactory.getLogger(ConstraintsRebalanceAlgorithm.class);

  private Map<HardConstraint.FailureReason, Integer> _failureReasonCounterMap = new HashMap<>();

  public ConstraintsRebalanceAlgorithm() {
    // TODO Constraints initialization
  }

  @Override
  public Map<String, ResourceAssignment> rebalance(ClusterModel clusterModel,
      Map<String, Map<HardConstraint.FailureReason, Integer>> failureReasons) {
    return new HashMap<>();
  }
}
