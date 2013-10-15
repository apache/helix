package org.apache.helix.model.builder;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.CurrentState.CurrentStateProperty;

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
 * Assemble a CurrentState
 */
public class CurrentStateBuilder {
  private final ResourceId _resourceId;
  private final Map<PartitionId, State> _partitionStateMap;
  private SessionId _sessionId;
  private StateModelDefId _stateModelDefId;
  private StateModelFactoryId _stateModelFactoryId;

  /**
   * Build a current state for a given resource
   * @param resourceId resource identifier
   */
  public CurrentStateBuilder(ResourceId resourceId) {
    _resourceId = resourceId;
    _partitionStateMap = new HashMap<PartitionId, State>();
  }

  /**
   * Add partition-state mappings for this instance and resource
   * @param mappings map of partition to state
   * @return CurrentStateBuilder
   */
  public CurrentStateBuilder addMappings(Map<PartitionId, State> mappings) {
    _partitionStateMap.putAll(mappings);
    return this;
  }

  /**
   * Add a single partition-state mapping for this instance and resource
   * @param partitionId the partition to map
   * @param state the replica state
   * @return CurrentStateBuilder
   */
  public CurrentStateBuilder addMapping(PartitionId partitionId, State state) {
    _partitionStateMap.put(partitionId, state);
    return this;
  }

  /**
   * Set the session id for this current state
   * @param sessionId session identifier
   * @return CurrentStateBuilder
   */
  public CurrentStateBuilder sessionId(SessionId sessionId) {
    _sessionId = sessionId;
    return this;
  }

  /**
   * Set the state model for this current state
   * @param stateModelDefId state model definition identifier
   * @return CurrentStateBuilder
   */
  public CurrentStateBuilder stateModelDef(StateModelDefId stateModelDefId) {
    _stateModelDefId = stateModelDefId;
    return this;
  }

  /**
   * Set the name of the state model factory
   * @param stateModelFactoryIde state model factory identifier
   * @return CurrentStateBuilder
   */
  public CurrentStateBuilder stateModelFactory(StateModelFactoryId stateModelFactoryId) {
    _stateModelFactoryId = stateModelFactoryId;
    return this;
  }

  /**
   * Create a CurrentState
   * @return instantiated CurrentState
   */
  public CurrentState build() {
    ZNRecord record = new ZNRecord(_resourceId.stringify());
    for (PartitionId partitionId : _partitionStateMap.keySet()) {
      Map<String, String> stateMap = new HashMap<String, String>();
      stateMap.put(CurrentStateProperty.CURRENT_STATE.toString(),
          _partitionStateMap.get(partitionId).toString());
      record.setMapField(partitionId.toString(), stateMap);
    }
    record.setSimpleField(CurrentStateProperty.SESSION_ID.toString(), _sessionId.toString());
    record.setSimpleField(CurrentStateProperty.STATE_MODEL_DEF.toString(),
        _stateModelDefId.toString());
    record.setSimpleField(CurrentStateProperty.STATE_MODEL_FACTORY_NAME.toString(),
        _stateModelFactoryId.toString());
    return new CurrentState(record);
  }
}
