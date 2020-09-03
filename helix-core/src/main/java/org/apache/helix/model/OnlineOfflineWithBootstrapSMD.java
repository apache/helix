package org.apache.helix.model;

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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.zookeeper.datamodel.ZNRecord;

/**
 * Helix built-in state model definition based on Online-Offline but with the additional bootstrap
 * state.
 */
public final class OnlineOfflineWithBootstrapSMD extends StateModelDefinition {
  public static final String name = "OnlineOfflineWithBootstrap";

  /**
   * Instantiate from a pre-populated record
   *
   * @param record ZNRecord representing a state model definition
   */
  private OnlineOfflineWithBootstrapSMD(ZNRecord record) {
    super(record);
  }

  public enum States {
    ONLINE, BOOTSTRAP, OFFLINE
  }

  /**
   * Build OnlineOfflineWithBootstrap state model definition
   *
   * @return
   */
  public static OnlineOfflineWithBootstrapSMD build() {
    Builder builder = new Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.ONLINE.name(), 0);
    builder.addState(States.BOOTSTRAP.name(), 1);
    builder.addState(States.OFFLINE.name(), 2);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.ONLINE.name(), States.OFFLINE.name(), 0);
    builder.addTransition(States.OFFLINE.name(), States.BOOTSTRAP.name(), 1);
    builder.addTransition(States.BOOTSTRAP.name(), States.ONLINE.name(), 2);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.dynamicUpperBound(States.ONLINE.name(), "R");

    return new OnlineOfflineWithBootstrapSMD(builder.build().getRecord());
  }
}
