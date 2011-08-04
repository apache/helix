package com.linkedin.clustermanager.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.clustermanager.model.ResourceKey;

public class BestPossibleStateOutput
{
  Map<String, Map<ResourceKey, Map<String, String>>> _dataMap;

  public BestPossibleStateOutput()
  {
    _dataMap = new HashMap<String, Map<ResourceKey, Map<String, String>>>();
  }

  public void setState(String resourceGroupName, ResourceKey resource,
      Map<String, String> bestInstanceStateMappingForResource)
  {
    if (!_dataMap.containsKey(resourceGroupName))
    {
      _dataMap.put(resourceGroupName,
          new HashMap<ResourceKey, Map<String, String>>());
    }
    Map<ResourceKey, Map<String, String>> map = _dataMap.get(resourceGroupName);
    map.put(resource, bestInstanceStateMappingForResource);
  }

  public Map<String, String> getInstanceStateMap(String resourceGroupName,
      ResourceKey resource)
  {
    Map<ResourceKey, Map<String, String>> map = _dataMap.get(resourceGroupName);
    if (map != null)
    {
      return map.get(resource);
    }
    return Collections.emptyMap();
  }

  public String toString()
  {
    return _dataMap.toString();
  }
}
