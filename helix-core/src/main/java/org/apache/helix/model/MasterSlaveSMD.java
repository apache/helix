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
 * Helix built-in Master-slave state model definition
 */
public final class MasterSlaveSMD extends StateModelDefinition {
  public static final String name = "MasterSlave";

  public enum States {
    MASTER,
    SLAVE,
    OFFLINE
  }

  public MasterSlaveSMD() {
    super(generateConfigForMasterSlave());
  }

  /**
   * Build Master-slave state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder =new StateModelDefinition.Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.MASTER.name(), 0);
    builder.addState(States.SLAVE.name(), 1);
    builder.addState(States.OFFLINE.name(), 2);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.MASTER.name(), States.SLAVE.name(), 0);
    builder.addTransition(States.SLAVE.name(), States.MASTER.name(), 1);
    builder.addTransition(States.OFFLINE.name(), States.SLAVE.name(), 2);
    builder.addTransition(States.SLAVE.name(), States.OFFLINE.name(), 3);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.upperBound(States.MASTER.name(), 1);
    builder.dynamicUpperBound(States.SLAVE.name(), "R");

    return builder.build();
  }

  /**
   * Generate Master-slave state model definition
   * Replaced by MasterSlaveSMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForMasterSlave() {
    ZNRecord record = new ZNRecord("MasterSlave");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("MASTER");
    statePriorityList.add("SLAVE");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    statePriorityList.add("ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("MASTER")) {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      } else if (state.equals("SLAVE")) {
        metadata.put("count", "R");
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
        metadata.put("SLAVE", "SLAVE");
        metadata.put("OFFLINE", "SLAVE");
        metadata.put("DROPPED", "SLAVE");
        record.setMapField(key, metadata);
      } else if (state.equals("SLAVE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("MASTER", "MASTER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("SLAVE", "SLAVE");
        metadata.put("MASTER", "SLAVE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("MASTER-SLAVE");
    stateTransitionPriorityList.add("SLAVE-MASTER");
    stateTransitionPriorityList.add("OFFLINE-SLAVE");
    stateTransitionPriorityList.add("SLAVE-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }
}
