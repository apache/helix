package com.linkedin.clustermanager.controller;

import com.linkedin.clustermanager.model.StateModelDefinition;

public class RelayStateModelDefinition extends StateModelDefinition
{

  @Override
  public String getNextStateForTransition(String fromState, String toState)
  {
    String nextState = null;
    if (fromState.equals("OFFLINE"))
    {
      if (toState.equals("ONLINE"))
      {
        nextState = "ONLINE";
      }
    }
    if (fromState.equals("ONLINE"))
    {
      if (toState.equals("OFFLINE"))
      {
        nextState = "OFFLINE";
      }
    }
    if (nextState == null)
    {
      System.err.println("Unable to compute nextState to go from:" + fromState
          + " to:" + toState);
    }
    return nextState;
  }

  @Override
  public String getInitialState()
  {
    return "OFFLINE";
  }

}
