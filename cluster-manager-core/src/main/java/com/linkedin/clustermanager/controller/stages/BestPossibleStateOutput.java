package com.linkedin.clustermanager.controller.stages;

import java.util.Map;

import com.linkedin.clustermanager.model.ResourceKey;

public class BestPossibleStateOutput
{

  public void setState(String resourceGroupName, ResourceKey resource,
      Map<String, String> bestInstanceStateMappingForResource)
  {
    
  }

  public Map<String, String> getInstanceStateMap(String resourceGroupName, ResourceKey resource){
    return null;
  }
}
