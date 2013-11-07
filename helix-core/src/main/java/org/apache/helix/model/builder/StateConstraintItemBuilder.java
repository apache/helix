package org.apache.helix.model.builder;

import org.apache.helix.api.State;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ConstraintItem;

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
 * Build a ConstraintItem corresponding to a state bound
 */
public class StateConstraintItemBuilder {
  private ConstraintItemBuilder _builder;

  /**
   * Instantiate the builder
   */
  public StateConstraintItemBuilder() {
    _builder = new ConstraintItemBuilder();
  }

  /**
   * Get the state model definition to constrain
   * @param stateModelDefId state model definition identifier
   * @return StateConstraintItemBuilder
   */
  public StateConstraintItemBuilder stateModel(StateModelDefId stateModelDefId) {
    _builder.addConstraintAttribute(ConstraintAttribute.STATE_MODEL.toString(),
        stateModelDefId.stringify());
    return this;
  }

  /**
   * Get the state to constrain
   * @param state state object
   * @return StateConstraintItemBuilder
   */
  public StateConstraintItemBuilder state(State state) {
    _builder.addConstraintAttribute(ConstraintAttribute.STATE.toString(), state.toString());
    return this;
  }

  /**
   * Set a numerical upper bound for the replicas that can be in a state
   * @param upperBound maximum replica count for a state, per partition
   * @return StateConstraintItemBuilder
   */
  public StateConstraintItemBuilder upperBound(int upperBound) {
    _builder.addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(),
        Integer.toString(upperBound));
    return this;
  }

  /**
   * Set an upper bound for the replicas that can be in a state. This can be numerical, or "N" for
   * number of nodes, or "R" for total number of replicas per partition
   * @param dynamicUpperBound maximum replica count for a state, per partition, can also be "N" or
   *          "R"
   * @return StateConstraintItemBuilder
   */
  public StateConstraintItemBuilder dynamicUpperBound(String dynamicUpperBound) {
    _builder.addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(),
        dynamicUpperBound);
    return this;
  }

  /**
   * Get the ConstraintItem instance that is built
   * @return ConstraintItem
   */
  public ConstraintItem build() {
    return _builder.build();
  }
}
