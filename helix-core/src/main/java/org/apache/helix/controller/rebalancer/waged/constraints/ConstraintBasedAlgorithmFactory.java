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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.controller.rebalancer.waged.RebalanceAlgorithm;
import org.apache.helix.model.ClusterConfig;

public class ConstraintBasedAlgorithmFactory {

  // TODO: the parameter comes from cluster config, will tune how these 2 integers will change the
  // soft constraint weight model
  public static RebalanceAlgorithm getInstance() {
    // TODO initialize constraints, depending on constraints implementations PRs
    List<HardConstraint> hardConstraints = new ArrayList<>();
    List<SoftConstraint> softConstraints = new ArrayList<>();
    SoftConstraintWeightModel softConstraintWeightModel = new SoftConstraintWeightModel();

    return new ConstraintBasedAlgorithm(hardConstraints, softConstraints,
        softConstraintWeightModel);
  }
}
