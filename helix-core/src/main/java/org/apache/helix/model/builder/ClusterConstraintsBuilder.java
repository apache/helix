package org.apache.helix.model.builder;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.id.ConstraintId;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;

public class ClusterConstraintsBuilder {
  final private ConstraintType _constraintType;

  /**
   * constraint-id -> constraint-item-builder
   * e.g. constraint_1: at most 1 offline->slave state transition message for MyDB:
   * constraint_1:
   * MESSAGE_TYPE : StateTransition
   * RESOURCE : MyDB
   * TRANSITION : OFFLINE->SLAVE
   * CONSTRAINT_VALUE : 1
   */
  private final Map<ConstraintId, ConstraintItemBuilder> _constraintBuilderMap =
      new HashMap<ConstraintId, ConstraintItemBuilder>();

  public ClusterConstraintsBuilder(ConstraintType type) {
    if (type == null) {
      throw new IllegalArgumentException("constraint-type should NOT be null");
    }
    _constraintType = type;
  }

  public ClusterConstraintsBuilder addConstraintAttribute(ConstraintId constraintId,
      String attribute, String value) {
    if (!_constraintBuilderMap.containsKey(constraintId)) {
      _constraintBuilderMap.put(constraintId, new ConstraintItemBuilder());
    }
    ConstraintItemBuilder builder = _constraintBuilderMap.get(constraintId);
    builder.addConstraintAttribute(attribute, value);
    return this;
  }

  public ClusterConstraintsBuilder addConstraintAttribute(String constraintId, String attribute,
      String value) {
    return addConstraintAttribute(ConstraintId.from(constraintId), attribute, value);
  }

  public ClusterConstraints build() {
    ClusterConstraints constraints = new ClusterConstraints(_constraintType);

    for (ConstraintId constraintId : _constraintBuilderMap.keySet()) {
      ConstraintItemBuilder builder = _constraintBuilderMap.get(constraintId);
      constraints.addConstraintItem(constraintId, builder.build());
    }
    return constraints;
  }
}
