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

import org.apache.helix.controller.rebalancer.waged.model.AssignableNode;
import org.apache.helix.controller.rebalancer.waged.model.AssignableReplica;
import org.apache.helix.controller.rebalancer.waged.model.ClusterContext;

/**
 * Evaluate a partition allocation proposal and return a score within the normalized range.
 * A higher score means the proposal is more preferred.
 */
public interface SoftConstraint {
  float MIN_SCORE = -1000.0f;
  float MAX_SCORE = 1000.0f;

  /**
   * The scoring function returns a score between MINIMAL_SCORE and MAXIMUM_SCORE, which is then weighted by the
   * individual normalized constraint weights.
   * Each individual constraint will define the meaning of MINIMAL_SCORE to MAXIMUM_SCORE differently.
   */
  float assignmentScore(AssignableNode node, AssignableReplica rep, ClusterContext clusterContext);

  /**
   * Set the importance factor of the soft constraint.
   * The more important it is, the more contribution it will make to the final evaluation.
   * @param importance
   */
  void setConstraintImportance(float importance);

  float getConstraintImportance();
}
