package org.apache.helix.api;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;

public class StateModelDefinitionAccessor {
  private static Logger LOG = Logger.getLogger(StateModelDefinitionAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;

  /**
   * @param accessor
   */
  public StateModelDefinitionAccessor(HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
  }

  /**
   * Get all of the state model definitions available to the cluster
   * @return map of state model ids to state model definition objects
   */
  public Map<StateModelDefId, StateModelDefinition> readStateModelDefinitions() {
    Map<String, StateModelDefinition> stateModelDefs =
        _accessor.getChildValuesMap(_keyBuilder.stateModelDefs());
    Map<StateModelDefId, StateModelDefinition> stateModelDefMap =
        new HashMap<StateModelDefId, StateModelDefinition>();

    for (String stateModelDefName : stateModelDefs.keySet()) {
      stateModelDefMap.put(new StateModelDefId(stateModelDefName),
          stateModelDefs.get(stateModelDefName));
    }

    return ImmutableMap.copyOf(stateModelDefMap);
  }

  /**
   * Add a state model definition. Updates the existing state model definition if it already exists.
   * @param stateModelDef fully initialized state model definition
   * @return true if the model is persisted, false otherwise
   */
  public boolean addStateModelDefinition(StateModelDefinition stateModelDef) {
    return _accessor.setProperty(_keyBuilder.stateModelDef(stateModelDef.getId()), stateModelDef);
  }
}
