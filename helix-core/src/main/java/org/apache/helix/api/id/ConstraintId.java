package org.apache.helix.api.id;

import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.model.Transition;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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

/**
 * Identifies a constraint item on the cluster
 */
public class ConstraintId extends Id {
  @JsonProperty("id")
  private final String _id;

  /**
   * Create a constraint id
   * @param constraintId string representing the constraint id
   */
  @JsonCreator
  public ConstraintId(@JsonProperty("id") String id) {
    _id = id;
  }

  @Override
  public String stringify() {
    return _id;
  }

  /**
   * Get a constraint id from a string
   * @param constraintId string representing the constraint id
   * @return ConstraintId
   */
  public static ConstraintId from(String constraintId) {
    return new ConstraintId(constraintId);
  }

  /**
   * Get a state constraint id based on the state model definition and state
   * @param scope the scope of the constraint
   * @param stateModelDefId the state model
   * @param state the constrained state
   * @return ConstraintId
   */
  public static ConstraintId from(Scope<?> scope, StateModelDefId stateModelDefId, State state) {
    return new ConstraintId(scope + "|" + stateModelDefId + "|" + state);
  }

  /**
   * Get a state constraint id based on the state model definition and transition
   * @param scope the scope of the constraint
   * @param stateModelDefId the state model
   * @param transition the constrained transition
   * @return ConstraintId
   */
  public static ConstraintId from(Scope<?> scope, StateModelDefId stateModelDefId,
      Transition transition) {
    return new ConstraintId(scope + "|" + stateModelDefId + "|" + transition);
  }
}
