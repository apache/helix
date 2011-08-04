package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import com.linkedin.clustermanager.model.ResourceKey;

public class CurrentStateOutput
{

  public void setCurrentState(String resourceGroupName,
      ResourceKey resourceKey, String instanceName, String state)
  {
    // TODO Auto-generated method stub

  }

  public void setPendingState(String resourceGroupName,
      ResourceKey resourceKey, String instanceName, String state)
  {
    // TODO Auto-generated method stub
    
  }

  public String getCurrentState(String resourceGroupName, ResourceKey resource,
      String instanceName)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public String getPendingState(String resourceGroupName, ResourceKey resource,
      String instanceName)
  {
    // TODO Auto-generated method stub
    return null;
  }

  public Map<String, String> getCurrentStateMap(String resourceGroupName,
      ResourceKey resource)
  {
    // TODO Auto-generated method stub
    return null;
  }

}
