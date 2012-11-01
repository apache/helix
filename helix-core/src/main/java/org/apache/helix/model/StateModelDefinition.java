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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.participant.statemachine.StateTransitionTableBuilder;
import org.apache.log4j.Logger;

/**
 * Describe the state model
 */
public class StateModelDefinition extends HelixProperty
{
  public enum StateModelDefinitionProperty
  {
    INITIAL_STATE, STATE_TRANSITION_PRIORITYLIST, STATE_PRIORITY_LIST
  }

  private static final Logger _logger = Logger
      .getLogger(StateModelDefinition.class.getName());
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

  public StateModelDefinition(ZNRecord record)
  {
    super(record);

    _statesPriorityList = record
        .getListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST
            .toString());
    _stateTransitionPriorityList = record
        .getListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST
            .toString());
    _stateTransitionTable = new HashMap<String, Map<String, String>>();
    _statesCountMap = new HashMap<String, String>();
    if (_statesPriorityList != null)
    {
      for (String state : _statesPriorityList)
      {
        Map<String, String> metaData = record.getMapField(state + ".meta");
        if (metaData != null)
        {
          if (metaData.get("count") != null)
          {
            _statesCountMap.put(state, metaData.get("count"));
          }
        }
        Map<String, String> nextData = record.getMapField(state + ".next");
        _stateTransitionTable.put(state, nextData);
      }
    }
  }

  public List<String> getStateTransitionPriorityList()
  {
    return _stateTransitionPriorityList;
  }

  public List<String> getStatesPriorityList()
  {
    return _statesPriorityList;
  }

  public String getNextStateForTransition(String fromState, String toState)
  {
    Map<String, String> map = _stateTransitionTable.get(fromState);
    if (map != null)
    {
      return map.get(toState);
    }
    return null;
  }

  public String getInitialState()
  {
    return _record.getSimpleField(StateModelDefinitionProperty.INITIAL_STATE
        .toString());
  }

  public String getNumInstancesPerState(String state)
  {
    return _statesCountMap.get(state);
  }

  @Override
  public boolean isValid()
  {
    if (getInitialState() == null)
    {
      _logger.error("State model does not contain init state, statemodel:"
          + _record.getId());
      return false;
    }
    if (_record.getListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST
        .toString()) == null)
    {
      _logger
          .error("CurrentState does not contain StatesPriorityList, state model : "
              + _record.getId());
      return false;
    }
    return true;
  }

  /**
   * 
   *
   */
  public static class Builder
  {
    private final String _statemodelName;
    private String initialState;
    Map<String, Integer> statesMap;
    Map<Transition, Integer> transitionMap;
    Map<String, String> stateConstraintMap;

    public Builder(String name)
    {
      this._statemodelName = name;
      statesMap = new HashMap<String, Integer>();
      transitionMap = new HashMap<Transition, Integer>();
      stateConstraintMap = new HashMap<String, String>();
    }

    /**
     * initial state of a replica when it starts, most commonly used initial
     * state is OFFLINE
     * 
     * @param state
     */
    public void initialState(String initialState)
    {
      this.initialState = initialState;

    }

    /**
     * Define all valid states using this method. Set the priority in which the
     * constraints must be satisfied. Lets say STATE1 has a constraint of 1 and
     * STATE2 has a constraint of 3 but only one node is up then Helix will uses
     * the priority to see STATE constraint has to be given higher preference <br/>
     * Use -1 to indicates states with no constraints, like OFFLINE
     * 
     * @param states
     */
    public void addState(String state, int priority)
    {
      statesMap.put(state, priority);
    }

    /**
     * Define all legal transitions between states using this method. Priority
     * is used to order the transitions. Helix tries to maximize the number of
     * transitions that can be fired in parallel without violating the
     * constraint. The transitions are first sorted based on priority and
     * transitions are selected in a greedy way until the constriants are not
     * violated.
     * 
     * @param fromState
     * @param toState
     * @param priority
     */
    public void addTransition(String fromState, String toState, int priority)
    {
      transitionMap.put(new Transition(fromState, toState), priority);
    }

    public void upperBound(String state, int upperBound)
    {
      stateConstraintMap.put(state, String.valueOf(upperBound));
    }

    /**
     * You can use this to have the bounds dynamically change based on other
     * parameters. Currently support 2 values R --> Refers to the number of
     * replicas specified during resource creation. This allows having different
     * replication factor for each resource without having to create a different
     * state machine. N --> Refers to all nodes in the cluster. Useful for
     * resources that need to exist on all nodes. This way one can add/remove
     * nodes without having the change the bounds.
     * 
     * @param state
     * @param bound
     */
    public void dynamicUpperBound(String state, String bound)
    {
      stateConstraintMap.put(state, bound);
    }

    public StateModelDefinition build()
    {
      ZNRecord record = new ZNRecord(_statemodelName);
      ArrayList<String> statePriorityList = new ArrayList<String>(
          statesMap.keySet());
      Comparator<? super String> c1 = new Comparator<String>()
      {

        @Override
        public int compare(String o1, String o2)
        {
          return statesMap.get(o1).compareTo(statesMap.get(o2));
        }
      };
      Collections.sort(statePriorityList, c1);
      ArrayList<String> transitionPriorityList = new ArrayList<String>(
          transitionMap.size());
      for (Transition t : transitionMap.keySet())
      {
        transitionPriorityList.add(t.toString());
      }
      Comparator<? super String> c2 = new Comparator<String>()
      {

        @Override
        public int compare(String o1, String o2)
        {
          return statesMap.get(o1).compareTo(statesMap.get(o2));
        }
      };
      Collections.sort(transitionPriorityList, c2);

      record.setSimpleField(
          StateModelDefinitionProperty.INITIAL_STATE.toString(), initialState);
      record.setListField(
          StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
          statePriorityList);
      record
          .setListField(
              StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST
                  .toString(), transitionPriorityList);
      StateTransitionTableBuilder stateTransitionTableBuilder = new StateTransitionTableBuilder();
      Map<String, Map<String, String>> transitionTable = stateTransitionTableBuilder
          .buildTransitionTable(statePriorityList, new ArrayList<Transition>(
              transitionMap.keySet()));
      for (String state : transitionTable.keySet())
      {
        record.setMapField(state + ".next", transitionTable.get(state));
      }
      for (String state : stateConstraintMap.keySet())
      {
        HashMap<String, String> metadata = new HashMap<String, String>();
        metadata.put("count", stateConstraintMap.get(state));
        record.setMapField(state + ".meta", metadata);
      }
      return new StateModelDefinition(record);
    }

  }

}
