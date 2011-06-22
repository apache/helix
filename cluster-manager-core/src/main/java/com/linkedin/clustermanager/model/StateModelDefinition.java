package com.linkedin.clustermanager.model;

import java.util.List;
import java.util.Map;


public class StateModelDefinition
{
    /**
     * State Names in priority order. Indicates the order in which states are fulfilled
     */
    private List<String> _statesPriorityList;
    /**
     * Specifies the max number of instances for a given state
     */
    private Map<String, Integer> _statesMaxCountMap;
    /**
     * Min number of instances in a given state
     */
    private Map<String, Integer> _statesMinCountMap;
    /**
     * StateTransition which is used to find the nextState given StartState and FinalState
     */
    private Map<String, Map<String, Map<String, String>>> _stateTransitionTable;

    
    public String getNextStateForTransition(String fromState, String toState)
    {
        return null;
    }

    public String getInitialState()
    {
        return null;
    }

}
