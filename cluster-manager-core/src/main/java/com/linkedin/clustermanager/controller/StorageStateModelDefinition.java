package com.linkedin.clustermanager.controller;

import com.linkedin.clustermanager.model.StateModelDefinition;


public class StorageStateModelDefinition extends StateModelDefinition
{

    @Override
    public String getNextStateForTransition(String fromState, String toState)
    {
        String nextState = null;
        if (fromState.equals("OFFLINE"))
        {
            if (toState.equals("SLAVE"))
            {
                nextState = "SLAVE";
            }
            if (toState.equals("MASTER"))
            {
                nextState = "SLAVE";
            }

        }
        if (fromState.equals("SLAVE"))
        {
            if (toState.equals("OFFLINE"))
            {
                nextState = "OFFLINE";
            }
            if (toState.equals("MASTER"))
            {
                nextState = "MASTER";
            }

        }
        if (fromState.equals("MASTER"))
        {
            if (toState.equals("OFFLINE"))
            {
                nextState = "SLAVE";
            }
            if (toState.equals("SLAVE"))
            {
                nextState = "SLAVE";
            }
        }
        if (nextState == null)
        {
            System.err.println("Unable to compute nextState to go from:"
                    + fromState + " to:" + toState);
        }
        return nextState;
    }

    @Override
    public String getInitialState()
    {
        return "OFFLINE";
    }

}
