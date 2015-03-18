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
 * Helix built-in Leader-standby state model definition
 */
public final class LeaderStandbySMD extends StateModelDefinition {
  public static final String name = "LeaderStandby";
  public enum States {
    LEADER,
    STANDBY,
    OFFLINE
  }

  public LeaderStandbySMD() {
    super(generateConfigForLeaderStandby());
  }

  /**
   * Build Leader-standby state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder =new StateModelDefinition.Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.LEADER.name(), 0);
    builder.addState(States.STANDBY.name(), 1);
    builder.addState(States.OFFLINE.name(), 2);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.LEADER.name(), States.STANDBY.name(), 0);
    builder.addTransition(States.STANDBY.name(), States.LEADER.name(), 1);
    builder.addTransition(States.OFFLINE.name(), States.STANDBY.name(), 2);
    builder.addTransition(States.STANDBY.name(), States.OFFLINE.name(), 3);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.upperBound(States.LEADER.name(), 1);
    builder.dynamicUpperBound(States.STANDBY.name(), "R");

    return builder.build();
  }

  /**
   * Generate Leader-standby state model definition
   * Replaced by LeaderStandbySMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForLeaderStandby() {
    ZNRecord record = new ZNRecord("LeaderStandby");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("LEADER");
    statePriorityList.add("STANDBY");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("LEADER")) {
        metadata.put("count", "1");
        record.setMapField(key, metadata);
      }
      if (state.equals("STANDBY")) {
        metadata.put("count", "R");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }
      if (state.equals("DROPPED")) {
        metadata.put("count", "-1");
        record.setMapField(key, metadata);
      }

    }

    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("LEADER")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("STANDBY", "STANDBY");
        metadata.put("OFFLINE", "STANDBY");
        metadata.put("DROPPED", "STANDBY");
        record.setMapField(key, metadata);
      }
      if (state.equals("STANDBY")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("LEADER", "LEADER");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      }
      if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("STANDBY", "STANDBY");
        metadata.put("LEADER", "STANDBY");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      }

    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("LEADER-STANDBY");
    stateTransitionPriorityList.add("STANDBY-LEADER");
    stateTransitionPriorityList.add("OFFLINE-STANDBY");
    stateTransitionPriorityList.add("STANDBY-OFFLINE");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }
}
