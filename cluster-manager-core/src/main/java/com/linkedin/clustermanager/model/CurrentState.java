package com.linkedin.clustermanager.model;

import java.util.Map;

import com.linkedin.clustermanager.ZNRecord;

public class CurrentState
{
  private final ZNRecord record;

  public CurrentState(ZNRecord record)
  {
    this.record = record;
  }

  public String getResourceGroupName()
  {
    return null;
  }

  public Map<String,String> getResourceKeyStateMap()
  {
    return null;
  }

  public String getSessionId()
  {
    return null;
  }

  public String getState(String resourceKeyStr)
  {
    // TODO Auto-generated method stub
    return null;
  }

  
}
