package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.model.ResourceKey;

public class CurrentStateOutput
{
  private final Map<String, Map<ResourceKey, Map<String, String>>> _currentStateMap;
  private final Map<String, Map<ResourceKey, Map<String, String>>> _pendingStateMap;
  private final Map<String, String> _resourceGroupStateModelMap;

  public CurrentStateOutput()
  {
    _currentStateMap = new HashMap<String, Map<ResourceKey, Map<String, String>>>();
    _pendingStateMap = new HashMap<String, Map<ResourceKey, Map<String, String>>>();
    _resourceGroupStateModelMap = new HashMap<String, String>();
  }
  
  public void setResourceGroupStateModelDef(String resourceGroupName, String stateModelDefName)
  {
    _resourceGroupStateModelMap.put(resourceGroupName, stateModelDefName);
  }
  
  public String getResourceGroupStateModelDef(String resourceGroupName)
  {
    return _resourceGroupStateModelMap.get(resourceGroupName);
  }

  public void setCurrentState(String resourceGroupName,
      ResourceKey resourceKey, String instanceName, String state)
  {
    if (!_currentStateMap.containsKey(resourceGroupName))
    {
      _currentStateMap.put(resourceGroupName,
          new HashMap<ResourceKey, Map<String, String>>());
    }
    if (!_currentStateMap.get(resourceGroupName).containsKey(resourceKey))
    {
      _currentStateMap.get(resourceGroupName).put(resourceKey,
          new HashMap<String, String>());
    }
    _currentStateMap.get(resourceGroupName).get(resourceKey)
        .put(instanceName, state);
  }

  public void setPendingState(String resourceGroupName,
      ResourceKey resourceKey, String instanceName, String state)
  {
    if (!_pendingStateMap.containsKey(resourceGroupName))
    {
      _pendingStateMap.put(resourceGroupName,
          new HashMap<ResourceKey, Map<String, String>>());
    }
    if (!_pendingStateMap.get(resourceGroupName).containsKey(resourceKey))
    {
      _pendingStateMap.get(resourceGroupName).put(resourceKey,
          new HashMap<String, String>());
    }
    _pendingStateMap.get(resourceGroupName).get(resourceKey)
        .put(instanceName, state);
  }

  public String getCurrentState(String resourceGroupName, ResourceKey resource,
      String instanceName)
  {
    Map<ResourceKey, Map<String, String>> map = _currentStateMap
        .get(resourceGroupName);
    if (map != null)
    {
      Map<String, String> instanceStateMap = map.get(resource);
      if (instanceStateMap != null)
      {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  public String getPendingState(String resourceGroupName, ResourceKey resource,
      String instanceName)
  {
    Map<ResourceKey, Map<String, String>> map = _pendingStateMap
        .get(resourceGroupName);
    if (map != null)
    {
      Map<String, String> instanceStateMap = map.get(resource);
      if (instanceStateMap != null)
      {
        return instanceStateMap.get(instanceName);
      }
    }
    return null;
  }

  public Map<String, String> getCurrentStateMap(String resourceGroupName,
      ResourceKey resource)
  {
    Map<ResourceKey, Map<String, String>> map = _currentStateMap
        .get(resourceGroupName);
    if (map != null)
    {
      return map.get(resource);
    }
    return Collections.emptyMap();
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("current state= ").append(_currentStateMap);
    sb.append(", pending state= ").append(_pendingStateMap);
    return sb.toString();

  }

}
