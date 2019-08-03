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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.builder.StateTransitionTableBuilder;
import org.apache.helix.model.util.StateModelDefinitionValidator;

/**
 * Describe the state model
 */
public class StateModelDefinition extends HelixProperty {
  public enum StateModelDefinitionProperty {
    INITIAL_STATE,
    STATE_TRANSITION_PRIORITYLIST,
    STATE_PRIORITY_LIST
  }

  public static final int TOP_STATE_PRIORITY = 1;

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

  private Map<String, Integer> _statesPriorityMap = new HashMap<>();

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
    _stateTransitionTable = new HashMap<>();
    _statesCountMap = new HashMap<>();
    if (_statesPriorityList != null) {
      int priority = TOP_STATE_PRIORITY;
      for (String state : _statesPriorityList) {
        Map<String, String> metaData = record.getMapField(state + ".meta");
        if (metaData != null) {
          if (metaData.get("count") != null) {
            _statesCountMap.put(state, metaData.get("count"));
          }
        }
        Map<String, String> nextData = record.getMapField(state + ".next");
        _stateTransitionTable.put(state, nextData);
        _statesPriorityMap.put(state, priority++);
      }
    }

    // add HelixDefinedStates to statesPriorityMap in case it hasn't been added already
    for (HelixDefinedState state : HelixDefinedState.values()) {
      if (!_statesPriorityMap.containsKey(state.name())) {
        // Make it the lowest priority
        _statesPriorityMap.put(state.name(), Integer.MAX_VALUE);
      }
    }

    // add transitions for helix-defined states
    for (HelixDefinedState state : HelixDefinedState.values()) {
      if (_statesPriorityList == null || !_statesPriorityList.contains(state.toString())) {
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
   * Get an ordered priority list of transitions
   * @return transitions in the form SRC-DEST, the first of which is highest priority
   */
  public List<String> getStateTransitionPriorityList() {
    return _stateTransitionPriorityList;
  }

  public Map<String, Integer> getStatePriorityMap() {
    return _statesPriorityMap;
  }

  /**
   * Get an ordered priority list of states
   * @return state names, the first of which is highest priority
   */
  public List<String> getStatesPriorityList() {
    return _statesPriorityList;
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
   * Get the starting state in the model
   * @return name of the initial state
   */
  public String getInitialState() {
    return _initialState;
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
   * Get the top state of this state model
   * @return
   */
  public String getTopState() {
    return _statesPriorityList.get(0);
  }

  /**
   * Whether this state model allows at most a single replica in the top-state?
   *
   * @return
   */
  public boolean isSingleTopStateModel() {
    int topStateCount = 0;
    try {
      topStateCount = Integer.valueOf(_statesCountMap.get(getTopState()));
    } catch (NumberFormatException ex) {

    }

    return topStateCount == 1;
  }

  /**
   * Get the second top states, which need one step transition to top state
   * @return a set of second top states
   */
  public Set<String> getSecondTopStates() {
    Set<String> secondTopStates = new HashSet<String>();
    if (_statesPriorityList == null || _statesPriorityList.isEmpty()) {
      return secondTopStates;
    }
    String topState = _statesPriorityList.get(0);
    for (String state : _stateTransitionTable.keySet()) {
      Map<String, String> transitionMap = _stateTransitionTable.get(state);
      if (transitionMap != null && transitionMap.containsKey(topState) && transitionMap
          .get(topState).equals(topState)) {
        secondTopStates.add(state);
      }
    }
    return secondTopStates;
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
     * Start building a state model with a name
     * @param name state model name
     */
    public Builder(String name) {
      this._statemodelName = name;
      statesMap = new HashMap<>();
      transitionMap = new HashMap<>();
      stateConstraintMap = new HashMap<>();
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
     * @param state
     * @param priority
     */
    public Builder addState(String state, int priority) {
      statesMap.put(state, priority);
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
      List<String> transitionPriorityList = new ArrayList<>(transitionList.size());
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
              new ArrayList<>(transitionMap.keySet()));
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

  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }

    if (!(o instanceof StateModelDefinition)) {
      return false;
    }

    StateModelDefinition stateModelDefinition = (StateModelDefinition) o;
    return _initialState.equals(stateModelDefinition._initialState) && _statesCountMap
        .equals(stateModelDefinition._statesCountMap) && _statesPriorityList
        .equals(stateModelDefinition._statesPriorityList) && _stateTransitionPriorityList
        .equals(stateModelDefinition._stateTransitionPriorityList) &&
        _stateTransitionTable.equals(stateModelDefinition._stateTransitionTable);
  }

  /**
   * Get the state to its count map, order in its state priority.
   *
   * @return state count map: state->count
   */
  public LinkedHashMap<String, Integer> getStateCountMap(int candidateNodeNum, int totalReplicas) {
    LinkedHashMap<String, Integer> stateCountMap = new LinkedHashMap<>();
    List<String> statesPriorityList = getStatesPriorityList();

    int replicas = totalReplicas;
    for (String state : statesPriorityList) {
      String num = getNumInstancesPerState(state);
      if (candidateNodeNum <= 0) {
        break;
      }
      if ("N".equals(num)) {
        stateCountMap.put(state, candidateNodeNum);
        replicas -= candidateNodeNum;
        break;
      } else if ("R".equals(num)) {
        // wait until we get the counts for all other states
        continue;
      } else {
        int stateCount = -1;
        try {
          stateCount = Integer.parseInt(num);
        } catch (Exception e) {
        }

        if (stateCount > 0) {
          int count = stateCount <= candidateNodeNum ? stateCount : candidateNodeNum;
          candidateNodeNum -= count;
          stateCountMap.put(state, count);
          replicas -= count;
        }
      }
    }

    // get state count for R
    for (String state : statesPriorityList) {
      String num = getNumInstancesPerState(state);
      if ("R".equals(num)) {
        if (candidateNodeNum > 0 && replicas > 0) {
          stateCountMap.put(state, replicas < candidateNodeNum ? replicas : candidateNodeNum);
        }
        // should have at most one state using R
        break;
      }
    }
    return stateCountMap;
  }

  /**
   * Given instance->state map, return the state counts
   *
   * @param stateMap
   *
   * @return state->count map for the given state map.
   */
  public static Map<String, Integer> getStateCounts(Map<String, String> stateMap) {
    Map<String, Integer> stateCounts = new HashMap<>();
    for (String state : stateMap.values()) {
      if (!stateCounts.containsKey(state)) {
        stateCounts.put(state, 0);
      }
      stateCounts.put(state, stateCounts.get(state) + 1);
    }
    return stateCounts;
  }
}
