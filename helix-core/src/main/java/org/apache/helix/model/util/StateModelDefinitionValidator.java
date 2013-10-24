package org.apache.helix.model.util;

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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Validator logic for a StateModelDefinition.<br/>
 * <br/>
 * Usage:<br/>
 * StateModelDefinition stateModelDef = ...;<br/>
 * StateModelDefinitionValidator.isStateModelDefinitionValid(stateModelDef);
 */
public class StateModelDefinitionValidator {
  private static final Logger _logger = Logger.getLogger(StateModelDefinitionValidator.class);
  private final StateModelDefinition _stateModelDef;
  private final List<String> _statePriorityList;
  private final List<String> _transitionPriorityList;
  private final Set<String> _stateSet;

  /**
   * Instantiate a validator instance
   * @param stateModelDef the state model definition to validate
   */
  private StateModelDefinitionValidator(StateModelDefinition stateModelDef) {
    _stateModelDef = stateModelDef;
    _statePriorityList = stateModelDef.getStatesPriorityList();
    _transitionPriorityList = stateModelDef.getStateTransitionPriorityList();
    _stateSet = Sets.newHashSet(_statePriorityList);
  }

  /**
   * Check if the StateModelDefinition passes all validation checks
   * @return true if state model definition is valid, false otherwise
   */
  public boolean isStateModelDefinitionValid() {
    // has a name
    if (_stateModelDef.getId() == null || _stateModelDef.getId().isEmpty()) {
      _logger.error("State model does not have a name");
      return false;
    }

    // has an initial state
    if (_stateModelDef.getInitialState() == null || _stateModelDef.getInitialState().isEmpty()) {
      _logger
          .error("State model does not contain init state, statemodel:" + _stateModelDef.getId());
      return false;
    }

    // has states
    if (_statePriorityList == null || _statePriorityList.isEmpty()) {
      _logger.error("CurrentState does not contain StatesPriorityList, state model : "
          + _stateModelDef.getId());
      return false;
    }

    // initial state is a state
    if (!_stateSet.contains(_stateModelDef.getInitialState())) {
      _logger.error("Defined states does not include the initial state, state model: "
          + _stateModelDef.getId());
      return false;
    }

    // has a dropped state
    if (!_stateSet.contains(HelixDefinedState.DROPPED.toString())) {
      _logger.error("Defined states does not include the DROPPED state, state model: "
          + _stateModelDef.getId());
      return false;
    }

    // make sure individual checks all pass
    if (!areStateCountsValid() || !areNextStatesValid() || !isTransitionPriorityListValid()
        || !arePathsValid()) {
      return false;
    }

    return true;
  }

  /**
   * Check if state counts are properly defined for each state
   * @return true if state counts valid, false otherwise
   */
  private boolean areStateCountsValid() {
    for (String state : _statePriorityList) {
      // all states should have a count
      String count = _stateModelDef.getNumInstancesPerState(state);
      if (count == null) {
        _logger.error("State " + state + " needs an upper bound constraint, state model: "
            + _stateModelDef.getId());
        return false;
      }

      // count should be a number, N, or R
      try {
        Integer.parseInt(count);
      } catch (NumberFormatException e) {
        if (!count.equals("N") && !count.equals("R")) {
          _logger.error("State " + state + " has invalid count " + count + ", state model: "
              + _stateModelDef.getId());
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check if the state transition priority list is properly formed
   * @return true if the transition priority list is valid, false otherwise
   */
  private boolean isTransitionPriorityListValid() {
    if (_transitionPriorityList != null) {
      for (String transition : _transitionPriorityList) {
        // ensure that transition is of form FROM-TO
        int index = transition.indexOf('-');
        int lastIndex = transition.indexOf('-');
        if (index <= 0 || index >= transition.length() - 1 || index != lastIndex) {
          _logger.error("Transition " + transition + " is not of the form SRC-DEST, state model: "
              + _stateModelDef.getId());
          return false;
        }

        // from and to states should be valid states
        String from = transition.substring(0, index);
        String to = transition.substring(index + 1);
        if (!_stateSet.contains(from)) {
          _logger.error("State " + from + " in " + transition
              + " is not a defined state, state model" + _stateModelDef.getId());
          return false;
        }
        if (!_stateSet.contains(to)) {
          _logger.error("State " + to + " in " + transition
              + " is not a defined state, state model: " + _stateModelDef.getId());
          return false;
        }

        // the next state for the transition should be the to state
        if (!to.equals(_stateModelDef.getNextStateForTransition(from, to))) {
          _logger.error("Transition " + transition + " must have " + to + " as the next state");
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check if the "next" states in the state model definition are valid. These check the next values
   * at a single level. To check full paths, use {@link #arePathsValid()}.
   * @return true if next states are properly defined, false otherwise
   */
  private boolean areNextStatesValid() {
    for (String state : _statePriorityList) {
      // all states can reach DROPPED
      if (!state.equals(HelixDefinedState.DROPPED.toString())
          && _stateModelDef.getNextStateForTransition(state, HelixDefinedState.DROPPED.toString()) == null) {
        _logger.error("State " + state + " cannot reach the DROPPED state, state model: "
            + _stateModelDef.getId());
        return false;
      }

      // initial state should reach all states (other than error)
      if (!state.equals(_stateModelDef.getInitialState())
          && !state.equals(HelixDefinedState.ERROR.toString())
          && _stateModelDef.getNextStateForTransition(_stateModelDef.getInitialState(), state) == null) {
        _logger.error("Initial state " + _stateModelDef.getInitialState()
            + " should be able to reach all states, state model: " + _stateModelDef.getId());
        return false;
      }

      // validate "next" states
      for (String destState : _statePriorityList) {
        if (state.equals(destState)) {
          continue;
        }
        // the next state should exist
        String intermediate = _stateModelDef.getNextStateForTransition(state, destState);
        if (intermediate != null && !_stateSet.contains(intermediate)) {
          _logger.error("Intermediate state " + intermediate + " for transition " + state + "-"
              + destState + " is not a valid state, state model: " + _stateModelDef.getId());
          return false;
        }

        // the next state should not allow a self loop
        if (intermediate != null && intermediate.equals(state)) {
          _logger.error("Intermediate state " + intermediate + " for transition " + state + "-"
              + destState + " should never be the from state, state model: "
              + _stateModelDef.getId());
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check that the state model does not have loops or unreachable states and that next states
   * actually help make progress
   * @return true if the transitions are valid, false otherwise
   */
  private boolean arePathsValid() {
    // create a map for memoized path checking
    Map<String, Set<String>> alreadyChecked = Maps.newHashMap();
    for (String state : _statePriorityList) {
      alreadyChecked.put(state, new HashSet<String>());
    }

    // check all pairs for paths
    for (String from : _statePriorityList) {
      for (String to : _statePriorityList) {
        // ignore self transitions
        if (from.equals(to)) {
          continue;
        }

        // see if a path is claimed to exist
        Set<String> used = Sets.newHashSet(from);
        String next = _stateModelDef.getNextStateForTransition(from, to);
        if (next == null) {
          if (from.equals(_stateModelDef.getInitialState())
              && !to.equals(HelixDefinedState.ERROR.toString())) {
            _logger.error("Initial state " + from + " cannot reach " + to + ", state model: "
                + _stateModelDef.getId());
            return false;
          }
          continue;
        }
        // if a path exists, follow it all the way
        while (!to.equals(next)) {
          // no need to proceed if this path has already been traversed
          if (alreadyChecked.get(next).contains(to)) {
            break;
          }
          if (used.contains(next)) {
            _logger.error("Path from " + from + " to " + to
                + " contains an infinite loop, state model: " + _stateModelDef.getId());
            return false;
          }
          alreadyChecked.get(next).add(to);
          used.add(next);
          next = _stateModelDef.getNextStateForTransition(next, to);
          if (next == null) {
            _logger.error("Path from " + from + " to " + to + " is incomplete, state model: "
                + _stateModelDef.getId());
            return false;
          }
        }
        alreadyChecked.get(from).add(to);
      }
    }
    return true;
  }

  /**
   * Validate a StateModelDefinition instance
   * @param stateModelDef the state model definition to validate
   * @return true if the state model definition is valid, false otherwise
   */
  public static boolean isStateModelDefinitionValid(StateModelDefinition stateModelDef) {
    return new StateModelDefinitionValidator(stateModelDef).isStateModelDefinitionValid();
  }
}
