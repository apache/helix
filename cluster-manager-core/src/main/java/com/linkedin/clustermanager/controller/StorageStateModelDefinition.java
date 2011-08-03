package com.linkedin.clustermanager.controller;

import org.apache.log4j.Logger;

import com.linkedin.clustermanager.ZNRecord;
import com.linkedin.clustermanager.model.StateModelDefinition;

public class StorageStateModelDefinition extends StateModelDefinition
{
  public StorageStateModelDefinition(ZNRecord record)
  {
    super(record);
  }

  

  private static Logger logger = Logger.getLogger(StorageStateModelDefinition.class);
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
      logger.warn("Unable to compute nextState to go from:" + fromState
          + " to:" + toState + " . Using toState as next state");
      nextState = toState;
    }
    return nextState;
  }

  @Override
  public String getInitialState()
  {
    return "OFFLINE";
  }

}
