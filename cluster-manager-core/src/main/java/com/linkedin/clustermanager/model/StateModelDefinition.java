package com.linkedin.clustermanager.model;

import java.util.List;
import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public class StateModelDefinition
{

  public StateModelDefinition(ZNRecord record)
  {

  }

  /**
   * State Names in priority order. Indicates the order in which states are
   * fulfilled
   */
  private List<String> _statesPriorityList;

  /**
   * Specifies the number of instances for a given state <br>
   * -1 dont care, dont try to keep any resource in this state on any instance <br>
   * >0 any integer number greater than 0 specifies the number of instances needed to be in this state <br>
   * R all instances in the preference list can be in this state <br>
   * N all instances in the cluster will be put in this state.PreferenceList must be denoted as '*' 
   */
  private Map<String, Integer> _statesCountMap;
  
  private List<String> _stateTransitionPriorityList;

  public List<String> getStateTransitionPriorityList()
  {
    return _stateTransitionPriorityList;
  }

  public void setStateTransitionPriorityList(
      List<String> stateTransitionPriorityList)
  {
    _stateTransitionPriorityList = stateTransitionPriorityList;
  }

  /**
   * StateTransition which is used to find the nextState given StartState and
   * FinalState
   */
  private Map<String, Map<String, Map<String, String>>> _stateTransitionTable;

  public List<String> getStatesPriorityList()
  {
    return _statesPriorityList;
  }

  public void setStatesPriorityList(List<String> statesPriorityList)
  {
    _statesPriorityList = statesPriorityList;
  }

  public String getNextStateForTransition(String fromState, String toState)
  {
    return null;
  }

  public String getInitialState()
  {
    return null;
  }

  public String getNumInstancesPerState(String state)
  {
    return "";
  }

}
