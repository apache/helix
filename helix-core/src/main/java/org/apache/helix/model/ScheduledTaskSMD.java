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
import org.apache.helix.manager.zk.DefaultSchedulerMessageHandlerFactory;
import org.apache.helix.model.builder.StateTransitionTableBuilder;

/**
 * Helix built-in SchedulerTaskQueue state model definition
 */
public final class ScheduledTaskSMD extends StateModelDefinition {
  public static final String name = DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE;

  public enum States {
    COMPLETED,
    OFFLINE
  }

  public ScheduledTaskSMD() {
    super(generateConfigForScheduledTaskQueue());
  }

  /**
   * Build SchedulerTaskQueue state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder =new StateModelDefinition.Builder(name);
    // init state
    builder.initialState(States.OFFLINE.name());

    // add states
    builder.addState(States.COMPLETED.name(), 0);
    builder.addState(States.OFFLINE.name(), 1);
    for (HelixDefinedState state : HelixDefinedState.values()) {
      builder.addState(state.name());
    }

    // add transitions
    builder.addTransition(States.COMPLETED.name(), States.OFFLINE.name(), 0);
    builder.addTransition(States.OFFLINE.name(), States.COMPLETED.name(), 1);
    builder.addTransition(States.OFFLINE.name(), HelixDefinedState.DROPPED.name());

    // bounds
    builder.dynamicUpperBound(States.COMPLETED.name(), "1");

    return builder.build();
  }

  /**
   * Generate SchedulerTaskQueue state model definition
   * Replaced by SchedulerTaskQueueSMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForScheduledTaskQueue() {
    ZNRecord record = new ZNRecord(DefaultSchedulerMessageHandlerFactory.SCHEDULER_TASK_QUEUE);
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add("COMPLETED");
    statePriorityList.add("OFFLINE");
    statePriorityList.add("DROPPED");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      if (state.equals("COMPLETED")) {
        metadata.put("count", "1");
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

    List<String> states = new ArrayList<String>();
    states.add("COMPLETED");
    states.add("DROPPED");
    states.add("OFFLINE");

    List<Transition> transitions = new ArrayList<Transition>();
    transitions.add(new Transition("OFFLINE", "COMPLETED"));
    transitions.add(new Transition("OFFLINE", "DROPPED"));
    transitions.add(new Transition("COMPLETED", "DROPPED"));

    StateTransitionTableBuilder builder = new StateTransitionTableBuilder();
    Map<String, Map<String, String>> next = builder.buildTransitionTable(states, transitions);

    for (String state : statePriorityList) {
      String key = state + ".next";
      record.setMapField(key, next.get(state));
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add("OFFLINE-COMPLETED");
    stateTransitionPriorityList.add("OFFLINE-DROPPED");
    stateTransitionPriorityList.add("COMPLETED-DROPPED");

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);
    return record;
  }
}
