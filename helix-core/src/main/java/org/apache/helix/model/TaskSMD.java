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
import org.apache.helix.model.builder.StateTransitionTableBuilder;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;

/**
 * Helix built-in Task state model definition
 */
public final class TaskSMD extends StateModelDefinition {
  public TaskSMD() {
    super(generateConfigForTaskStateModel());
  }

  /**
   * Build Task state model definition
   * @return
   */
  public static StateModelDefinition build() {
    StateModelDefinition.Builder builder =new StateModelDefinition.Builder(TaskConstants.STATE_MODEL_NAME);
    // init state
    builder.initialState(TaskPartitionState.INIT.name());

    // add states
    builder.addState(TaskPartitionState.INIT.name(), 0);
    builder.addState(TaskPartitionState.RUNNING.name(), 1);
    builder.addState(TaskPartitionState.STOPPED.name(), 2);
    builder.addState(TaskPartitionState.COMPLETED.name(), 3);
    builder.addState(TaskPartitionState.TIMED_OUT.name(), 4);
    builder.addState(TaskPartitionState.TASK_ERROR.name(), 5);
    builder.addState(TaskPartitionState.TASK_ABORTED.name(), 6);
    builder.addState(TaskPartitionState.DROPPED.name());

    // add transitions
    builder.addTransition(TaskPartitionState.INIT.name(), TaskPartitionState.RUNNING.name(), 0);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.STOPPED.name(), 1);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.COMPLETED.name(), 2);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TIMED_OUT.name(), 3);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ERROR.name(), 4);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ABORTED.name(), 5);
    builder.addTransition(TaskPartitionState.STOPPED.name(), TaskPartitionState.RUNNING.name(), 6);

    // All states have a transition to DROPPED.
    builder.addTransition(TaskPartitionState.INIT.name(), TaskPartitionState.DROPPED.name(), 7);
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.DROPPED.name(), 8);
    builder.addTransition(TaskPartitionState.COMPLETED.name(), TaskPartitionState.DROPPED.name(), 9);
    builder.addTransition(TaskPartitionState.STOPPED.name(), TaskPartitionState.DROPPED.name(), 10);
    builder.addTransition(TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.DROPPED.name(), 11);
    builder.addTransition(TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.DROPPED.name(), 12);
    builder.addTransition(TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.DROPPED.name(), 13);


    // All states, except DROPPED, have a transition to INIT.
    builder.addTransition(TaskPartitionState.RUNNING.name(), TaskPartitionState.INIT.name(), 14);
    builder.addTransition(TaskPartitionState.COMPLETED.name(), TaskPartitionState.INIT.name(), 15);
    builder.addTransition(TaskPartitionState.STOPPED.name(), TaskPartitionState.INIT.name(), 16);
    builder.addTransition(TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.INIT.name(), 17);
    builder.addTransition(TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.INIT.name(), 18);
    builder.addTransition(TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.INIT.name(), 19);

    return builder.build();
  }

  /**
   * Generate Task state model definition
   * Replaced by TaskSMD#build()
   * @return
   */
  @Deprecated
  public static ZNRecord generateConfigForTaskStateModel() {
    ZNRecord record = new ZNRecord(TaskConstants.STATE_MODEL_NAME);

    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), TaskPartitionState.INIT.name());
    List<String> statePriorityList = new ArrayList<String>();
    statePriorityList.add(TaskPartitionState.INIT.name());
    statePriorityList.add(TaskPartitionState.RUNNING.name());
    statePriorityList.add(TaskPartitionState.STOPPED.name());
    statePriorityList.add(TaskPartitionState.COMPLETED.name());
    statePriorityList.add(TaskPartitionState.TIMED_OUT.name());
    statePriorityList.add(TaskPartitionState.TASK_ERROR.name());
    statePriorityList.add(TaskPartitionState.TASK_ABORTED.name());
    statePriorityList.add(TaskPartitionState.DROPPED.name());
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(), statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      metadata.put("count", "-1");
      record.setMapField(key, metadata);
    }

    List<String> states = new ArrayList<String>();
    states.add(TaskPartitionState.INIT.name());
    states.add(TaskPartitionState.RUNNING.name());
    states.add(TaskPartitionState.STOPPED.name());
    states.add(TaskPartitionState.COMPLETED.name());
    states.add(TaskPartitionState.TIMED_OUT.name());
    states.add(TaskPartitionState.TASK_ERROR.name());
    states.add(TaskPartitionState.TASK_ABORTED.name());
    states.add(TaskPartitionState.DROPPED.name());

    List<Transition> transitions = new ArrayList<Transition>();
    transitions.add(new Transition(TaskPartitionState.INIT.name(), TaskPartitionState.RUNNING.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.STOPPED.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.COMPLETED.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TIMED_OUT.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ERROR.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ABORTED.name()));
    transitions.add(new Transition(TaskPartitionState.STOPPED.name(), TaskPartitionState.RUNNING.name()));

    // All states have a transition to DROPPED.
    transitions.add(new Transition(TaskPartitionState.INIT.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.COMPLETED.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.STOPPED.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.DROPPED.name()));
    transitions.add(new Transition(TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.DROPPED.name()));


    // All states, except DROPPED, have a transition to INIT.
    transitions.add(new Transition(TaskPartitionState.RUNNING.name(), TaskPartitionState.INIT.name()));
    transitions.add(new Transition(TaskPartitionState.COMPLETED.name(), TaskPartitionState.INIT.name()));
    transitions.add(new Transition(TaskPartitionState.STOPPED.name(), TaskPartitionState.INIT.name()));
    transitions.add(new Transition(TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.INIT.name()));
    transitions.add(new Transition(TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.INIT.name()));
    transitions.add(new Transition(TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.INIT.name()));

    StateTransitionTableBuilder builder = new StateTransitionTableBuilder();
    Map<String, Map<String, String>> next = builder.buildTransitionTable(states, transitions);

    for (String state : statePriorityList) {
      String key = state + ".next";
      record.setMapField(key, next.get(state));
    }

    List<String> stateTransitionPriorityList = new ArrayList<String>();
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.INIT.name(), TaskPartitionState.RUNNING.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.STOPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.COMPLETED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.TIMED_OUT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ERROR.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.TASK_ABORTED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.STOPPED.name(), TaskPartitionState.RUNNING.name()));

    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.INIT.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.COMPLETED.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.STOPPED.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.DROPPED.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.DROPPED.name()));

    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.RUNNING.name(), TaskPartitionState.INIT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.COMPLETED.name(), TaskPartitionState.INIT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.STOPPED.name(), TaskPartitionState.INIT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.INIT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TASK_ERROR.name(), TaskPartitionState.INIT.name()));
    stateTransitionPriorityList.add(String.format("%s-%s", TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.INIT.name()));

    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
                        stateTransitionPriorityList);

    return record;
  }
}
