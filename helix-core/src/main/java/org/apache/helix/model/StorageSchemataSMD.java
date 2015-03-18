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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;

/**
 * Helix built-in StorageSchemata state model definition
 */
public final class StorageSchemataSMD extends StateModelDefinition {
  public static final String name = "STORAGE_DEFAULT_SM_SCHEMATA";
  public enum States {
    MASTER,
    OFFLINE
  }

  public StorageSchemataSMD() {
    super(generateConfigForStorageSchemata());
  }

  /**
   * Build StorageSchemata state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder =new StateModelDefinition.Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.MASTER.name(), 0);
    builder.addState(States.OFFLINE.name(), 1);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.MASTER.name(), States.OFFLINE.name(), 0);
    builder.addTransition(States.OFFLINE.name(), States.MASTER.name(), 1);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.dynamicUpperBound(States.MASTER.name(), "N");

    return builder.build();
  }

  /**
   * Generate StorageSchemata state model definition
   * Replaced by StorageSchemataSMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForStorageSchemata() {
    ZNRecord record = new ZNRecord("STORAGE_DEFAULT_SM_SCHEMATA");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("MASTER");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER")) {
        metadata.put("count", "N");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
    }
    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("MASTER")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }
      if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("MASTER-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-MASTER");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }
}
