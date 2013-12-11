package org.apache.helix.participant;

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

import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.participant.statemachine.HelixStateModelFactory;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;

/**
 * Helix participant uses this class to register/remove state model factory
 * State model factory creates state model that handles state transition messages
 */
public interface StateMachineEngine extends MessageHandlerFactory {

  /**
   * Replaced by {@link #registerStateModelFactory(StateModelDefId, HelixStateModelFactory)

   */
  @Deprecated
  public boolean registerStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory);

  /**
   * Replaced by {@link #registerStateModelFactory(StateModelDefId, String, HelixStateModelFactory)}
   */
  @Deprecated
  public boolean registerStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory, String factoryName);

  /**
   * Replaced by {@link #removeStateModelFactory(StateModelDefId, HelixStateModelFactory)}
   */
  @Deprecated
  public boolean removeStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory);

  /**
   * Replaced by {@link #removeStateModelFactory(StateModelDefId, String, HelixStateModelFactory)}
   */
  @Deprecated
  public boolean removeStateModelFactory(String stateModelDef,
      StateModelFactory<? extends StateModel> factory, String factoryName);

  /**
   * Register a default state model factory for a state model definition
   * A state model definition could be, for example:
   * "MasterSlave", "OnlineOffline", "LeaderStandby", etc.
   * Replacing {@link #registerStateModelFactory(String, StateModelFactory)}
   * @param stateModelDefId
   * @param factory
   * @return
   */
  public boolean registerStateModelFactory(StateModelDefId stateModelDefId,
      HelixStateModelFactory<? extends StateModel> factory);

  /**
   * Register a state model factory with a factory name for a state model definition
   * Replacing {@link #registerStateModelFactory(String, StateModelFactory, String)}
   * @param stateModelDefId
   * @param factoryName
   * @param factory
   * @return
   */
  public boolean registerStateModelFactory(StateModelDefId stateModelDefId, String factoryName,
      HelixStateModelFactory<? extends StateModel> factory);

/**
   * Remove the default state model factory for a state model definition
   * Replacing {@link #removeStateModelFactory(String, StateModelFactory)
   * @param stateModelDefId
   * @return
   */
  public boolean removeStateModelFactory(StateModelDefId stateModelDefId);

  /**
   * Remove the state model factory with a name for a state model definition
   * Replacing {@link #removeStateModelFactory(String, StateModelFactory, String)}
   * @param stateModelDefId
   * @param factoryName
   * @return
   */
  public boolean removeStateModelFactory(StateModelDefId stateModelDefId, String factoryName);

}
