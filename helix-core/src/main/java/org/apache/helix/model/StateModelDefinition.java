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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.builder.StateTransitionTableBuilder;
import org.apache.helix.model.util.StateModelDefinitionValidator;

import com.google.common.collect.ImmutableList;

/**
 * Describe the state model
 */
public class StateModelDefinition extends HelixProperty {
  public enum StateModelDefinitionProperty {
    INITIAL_STATE,
    STATE_TRANSITION_PRIORITYLIST,
    STATE_PRIORITY_LIST
  }

  /**
   * state model's initial state
   */
  private final String _initialState;

  /**
   * State Names in priority order. Indicates the order in which states are
   * fulfilled
   */
  private final List<String> _statesPriorityList;

  /**
   * Specifies the number of instances for a given state <br>
   * -1 don't care, don't try to keep any resource in this state on any instance <br>
   * >0 any integer number greater than 0 specifies the number of instances
   * needed to be in this state <br>
   * R all instances in the preference list can be in this state <br>
   * N all instances in the cluster will be put in this state.PreferenceList
   * must be denoted as '*'
   */
  private final Map<String, String> _statesCountMap;

  private final List<String> _stateTransitionPriorityList;

  /**
   * StateTransition which is used to find the nextState given StartState and
   * FinalState
   */
  private final Map<String, Map<String, String>> _stateTransitionTable;

  /**
   * Instantiate from a pre-populated record
   * @param record ZNRecord representing a state model definition
   */
  public StateModelDefinition(ZNRecord record) {
    super(record);

    _initialState = record.getSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString());

    if (_initialState == null) {
      throw new IllegalArgumentException("initial-state for " + record.getId() + " is null");
    }

    _statesPriorityList =
        record.getListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString());
    _stateTransitionPriorityList =
        record.getListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString());
    _stateTransitionTable = new HashMap<String, Map<String, String>>();
    _statesCountMap = new HashMap<String, String>();
    if (_statesPriorityList != null) {
      for (String state : _statesPriorityList) {
        Map<String, String> metaData = record.getMapField(state + ".meta");
        if (metaData != null) {
          if (metaData.get("count") != null) {
            _statesCountMap.put(state, metaData.get("count"));
          }
        }
        Map<String, String> nextData = record.getMapField(state + ".next");
        _stateTransitionTable.put(state, nextData);
      }
    }

    // add transitions for helix-defined states
    for (HelixDefinedState state : HelixDefinedState.values()) {
      if (!_statesPriorityList.contains(state.toString())) {
        _statesCountMap.put(state.toString(), "-1");
      }
    }

    addDefaultTransition(HelixDefinedState.ERROR.toString(), HelixDefinedState.DROPPED.toString(),
        HelixDefinedState.DROPPED.toString());
    addDefaultTransition(HelixDefinedState.ERROR.toString(), _initialState, _initialState);
    addDefaultTransition(_initialState, HelixDefinedState.DROPPED.toString(),
        HelixDefinedState.DROPPED.toString());
  }

  /**
   * add transitions involving helix-defines states
   * these transitions need not to be specified in state-model-definition
   * @param from source state
   * @param to destination state
   * @param next intermediate state to reach the destination
   */
  void addDefaultTransition(String from, String to, String next) {
    if (!_stateTransitionTable.containsKey(from)) {
      _stateTransitionTable.put(from, new TreeMap<String, String>());
    }

    if (!_stateTransitionTable.get(from).containsKey(to)) {
      _stateTransitionTable.get(from).put(to, next);
    }
  }

  /**
   * Get a concrete state model definition id
   * @return StateModelDefId
   */
  public StateModelDefId getStateModelDefId() {
    return StateModelDefId.from(getId());
  }

  /**
   * Get an ordered priority list of transitions
   * @return transitions in the form SRC-DEST, the first of which is highest priority
   */
  public List<String> getStateTransitionPriorityList() {
    return _stateTransitionPriorityList;
  }

  /**
   * Get an ordered priority list of transitions
   * @return Transition objects, the first of which is highest priority (immutable)
   */
  public List<Transition> getTypedStateTransitionPriorityList() {
    ImmutableList.Builder<Transition> builder = new ImmutableList.Builder<Transition>();
    for (String transition : getStateTransitionPriorityList()) {
      String fromState = transition.substring(0, transition.indexOf('-'));
      String toState = transition.substring(transition.indexOf('-') + 1);
      builder.add(Transition.from(State.from(fromState), State.from(toState)));
    }
    return builder.build();
  }

  /**
   * Get an ordered priority list of states
   * @return state names, the first of which is highest priority
   */
  public List<String> getStatesPriorityList() {
    return _statesPriorityList;
  }

  /**
   * Get an ordered priority list of states
   * @return immutable list of states, the first of which is highest priority (immutable)
   */
  public List<State> getTypedStatesPriorityList() {
    ImmutableList.Builder<State> builder = new ImmutableList.Builder<State>();
    for (String state : getStatesPriorityList()) {
      builder.add(State.from(state));
    }
    return builder.build();
  }

  /**
   * Get the intermediate state required to transition from one state to the other
   * @param fromState the source
   * @param toState the destination
   * @return the intermediate state
   */
  public String getNextStateForTransition(String fromState, String toState) {
    Map<String, String> map = _stateTransitionTable.get(fromState);
    if (map != null) {
      return map.get(toState);
    }
    return null;
  }

  /**
   * Get the intermediate state required to transition from one state to the other
   * @param fromState the source
   * @param toState the destination
   * @return the intermediate state, or null if not present
   */
  public State getNextStateForTransition(State fromState, State toState) {
    String next = getNextStateForTransition(fromState.toString(), toState.toString());
    if (next != null) {
      return State.from(getNextStateForTransition(fromState.toString(), toState.toString()));
    }
    return null;
  }

  /**
   * Get the starting state in the model
   * @return name of the initial state
   */
  public String getInitialState() {
    // return _record.getSimpleField(StateModelDefinitionProperty.INITIAL_STATE
    // .toString());
    return _initialState;
  }

  /**
   * Get the starting state in the model
   * @return name of the initial state
   */
  public State getTypedInitialState() {
    // return _record.getSimpleField(StateModelDefinitionProperty.INITIAL_STATE
    // .toString());
    return State.from(_initialState);
  }

  /**
   * Number of instances that can be in each state
   * @param state the state name
   * @return maximum instance count per state, can be "N" or "R"
   */
  public String getNumInstancesPerState(String state) {
    return _statesCountMap.get(state);
  }

  /**
   * Number of participants that can be in each state
   * @param state the state
   * @return maximum instance count per state, can be "N" or "R"
   */
  public String getNumParticipantsPerState(State state) {
    return _statesCountMap.get(state.toString());
  }

  @Override
  public boolean isValid() {
    return StateModelDefinitionValidator.isStateModelDefinitionValid(this);
  }

  // TODO move this to model.builder package, refactor StateModelConfigGenerator to use this
  /**
   * Construct a state model
   */
  public static class Builder {
    private final String _statemodelName;
    private String initialState;
    Map<String, Integer> statesMap;
    Map<Transition, Integer> transitionMap;
    Map<String, String> stateConstraintMap;

    /**
     * Start building a state model with a id
     * @param stateModelDefId state model id
     */
    public Builder(StateModelDefId stateModelDefId) {
      this._statemodelName = stateModelDefId.stringify();
      statesMap = new HashMap<String, Integer>();
      transitionMap = new HashMap<Transition, Integer>();
      stateConstraintMap = new HashMap<String, String>();
    }

    /**
     * Start building a state model with a name
     * @param stateModelDefId state model name
     */
    public Builder(String stateModelName) {
      this(StateModelDefId.from(stateModelName));
    }

    /**
     * initial state of a replica when it starts, most commonly used initial
     * state is OFFLINE
     * @param initialState
     */
    public Builder initialState(State initialState) {
      return initialState(initialState.toString());
    }

    /**
     * initial state of a replica when it starts, most commonly used initial
     * state is OFFLINE
     * @param initialState
     */
    public Builder initialState(String initialState) {
      this.initialState = initialState;
      return this;
    }

    /**
     * Define all valid states using this method. Set the priority in which the
     * constraints must be satisfied. Lets say STATE1 has a constraint of 1 and
     * STATE2 has a constraint of 3 but only one node is up then Helix will uses
     * the priority to see STATE constraint has to be given higher preference <br/>
     * Use -1 to indicates states with no constraints, like OFFLINE
     * @param state the state to add
     * @param priority the state priority, lower number is higher priority
     */
    public Builder addState(State state, int priority) {
      return addState(state.toString(), priority);
    }

    /**
     * Define all valid states using this method. Set the priority in which the
     * constraints must be satisfied. Lets say STATE1 has a constraint of 1 and
     * STATE2 has a constraint of 3 but only one node is up then Helix will uses
     * the priority to see STATE constraint has to be given higher preference <br/>
     * Use -1 to indicates states with no constraints, like OFFLINE
     * @param state the state to add
     * @param priority the state priority, lower number is higher priority
     */
    public Builder addState(String state, int priority) {
      statesMap.put(state, priority);
      return this;
    }

    /**
     * Sets the priority to Integer.MAX_VALUE
     * @param state
     */
    public Builder addState(State state) {
      addState(state, Integer.MAX_VALUE);
      return this;
    }

    /**
     * Sets the priority to Integer.MAX_VALUE
     * @param state
     */
    public Builder addState(String state) {
      addState(state, Integer.MAX_VALUE);
      return this;
    }

    /**
     * Define all legal transitions between states using this method. Priority
     * is used to order the transitions. Helix tries to maximize the number of
     * transitions that can be fired in parallel without violating the
     * constraint. The transitions are first sorted based on priority and
     * transitions are selected in a greedy way until the constriants are not
     * violated.
     * @param fromState source
     * @param toState destination
     * @param priority priority, higher value is higher priority
     * @return Builder
     */
    public Builder addTransition(State fromState, State toState, int priority) {
      transitionMap.put(new Transition(fromState, toState), priority);
      return this;
    }

    /**
     * Define all legal transitions between states using this method. Priority
     * is used to order the transitions. Helix tries to maximize the number of
     * transitions that can be fired in parallel without violating the
     * constraint. The transitions are first sorted based on priority and
     * transitions are selected in a greedy way until the constriants are not
     * violated.
     * @param fromState source
     * @param toState destination
     * @param priority priority, higher value is higher priority
     * @return Builder
     */
    public Builder addTransition(String fromState, String toState, int priority) {
      transitionMap.put(new Transition(fromState, toState), priority);
      return this;
    }

    /**
     * Add a state transition with maximal priority value
     * @see #addTransition(String, String, int)
     * @param fromState
     * @param toState
     * @return Builder
     */
    public Builder addTransition(State fromState, State toState) {
      addTransition(fromState, toState, Integer.MAX_VALUE);
      return this;
    }

    /**
     * Add a state transition with maximal priority value
     * @see #addTransition(String, String, int)
     * @param fromState
     * @param toState
     * @return Builder
     */
    public Builder addTransition(String fromState, String toState) {
      addTransition(fromState, toState, Integer.MAX_VALUE);
      return this;
    }

    /**
     * Set a maximum for replicas in this state
     * @param state state name
     * @param upperBound maximum
     * @return Builder
     */
    public Builder upperBound(State state, int upperBound) {
      return upperBound(state.toString(), upperBound);
    }

    /**
     * Set a maximum for replicas in this state
     * @param state state name
     * @param upperBound maximum
     * @return Builder
     */
    public Builder upperBound(String state, int upperBound) {
      stateConstraintMap.put(state, String.valueOf(upperBound));
      return this;
    }

    /**
     * You can use this to have the bounds dynamically change based on other
     * parameters. <br/>
     * Currently support 2 values <br/>
     * R --> Refers to the number of replicas specified during resource
     * creation. This allows having different replication factor for each
     * resource without having to create a different state machine. <br/>
     * N --> Refers to all nodes in the cluster. Useful for resources that need
     * to exist on all nodes. This way one can add/remove nodes without having
     * the change the bounds.
     * @param state
     * @param bound
     * @return Builder
     */
    public Builder dynamicUpperBound(State state, String bound) {
      return dynamicUpperBound(state.toString(), bound);
    }

    /**
     * You can use this to have the bounds dynamically change based on other
     * parameters. <br/>
     * Currently support 2 values <br/>
     * R --> Refers to the number of replicas specified during resource
     * creation. This allows having different replication factor for each
     * resource without having to create a different state machine. <br/>
     * N --> Refers to all nodes in the cluster. Useful for resources that need
     * to exist on all nodes. This way one can add/remove nodes without having
     * the change the bounds.
     * @param state
     * @param bound
     * @return Builder
     */
    public Builder dynamicUpperBound(String state, String bound) {
      stateConstraintMap.put(state, bound);
      return this;
    }

    /**
     * Create a StateModelDefinition from this Builder
     * @return StateModelDefinition
     */
    public StateModelDefinition build() {
      ZNRecord record = new ZNRecord(_statemodelName);

      // get sorted state priorities by specified values
      ArrayList<String> statePriorityList = new ArrayList<String>(statesMap.keySet());
      Comparator<? super String> c1 = new Comparator<String>() {

        @Override
        public int compare(String o1, String o2) {
          return statesMap.get(o1).compareTo(statesMap.get(o2));
        }
      };
      Collections.sort(statePriorityList, c1);

      // get sorted transition priorities by specified values
      ArrayList<Transition> transitionList = new ArrayList<Transition>(transitionMap.keySet());
      Comparator<? super Transition> c2 = new Comparator<Transition>() {
        @Override
        public int compare(Transition o1, Transition o2) {
          return transitionMap.get(o1).compareTo(transitionMap.get(o2));
        }
      };
      Collections.sort(transitionList, c2);
      List<String> transitionPriorityList = new ArrayList<String>(transitionList.size());
      for (Transition t : transitionList) {
        transitionPriorityList.add(t.toString());
      }

      record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), initialState);
      record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
          statePriorityList);
      record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
          transitionPriorityList);

      // compute full paths for next states
      StateTransitionTableBuilder stateTransitionTableBuilder = new StateTransitionTableBuilder();
      Map<String, Map<String, String>> transitionTable =
          stateTransitionTableBuilder.buildTransitionTable(statePriorityList,
              new ArrayList<Transition>(transitionMap.keySet()));
      for (String state : transitionTable.keySet()) {
        record.setMapField(state + ".next", transitionTable.get(state));
      }

      // state counts
      for (String state : statePriorityList) {
        HashMap<String, String> metadata = new HashMap<String, String>();
        if (stateConstraintMap.get(state) != null) {
          metadata.put("count", stateConstraintMap.get(state));
        } else {
          metadata.put("count", "-1");
        }
        record.setMapField(state + ".meta", metadata);
      }
      return new StateModelDefinition(record);
    }

  }

}
