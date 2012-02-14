package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.model.Partition;

public class BestPossibleStateOutput
{
  // resource->partition->instance->state
  Map<String, Map<Partition, Map<String, String>>> _dataMap;

  public BestPossibleStateOutput()
  {
    _dataMap = new HashMap<String, Map<Partition, Map<String, String>>>();
  }

  public void setState(String resourceName, Partition resource,
      Map<String, String> bestInstanceStateMappingForResource)
  {
    if (!_dataMap.containsKey(resourceName))
    {
      _dataMap.put(resourceName,
          new HashMap<Partition, Map<String, String>>());
    }
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    map.put(resource, bestInstanceStateMappingForResource);
  }

  public Map<String, String> getInstanceStateMap(String resourceName,
      Partition resource)
  {
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    if (map != null)
    {
      return map.get(resource);
    }
    return Collections.emptyMap();
  }

  public Map<Partition, Map<String, String>> getResourceMap(String resourceName)
  {
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    if (map != null)
    {
      return map;
    }
    return Collections.emptyMap();
  }

  @Override
  public String toString()
  {
    return _dataMap.toString();
  }
}
