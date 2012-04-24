/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.helix.model.Partition;

public class CurrentStateOutput
{
  private final Map<String, Map<Partition, Map<String, String>>> _currentStateMap;
  private final Map<String, Map<Partition, Map<String, String>>> _pendingStateMap;
  private final Map<String, String> _resourceStateModelMap;

  public CurrentStateOutput()
  {
    _currentStateMap = new HashMap<String, Map<Partition, Map<String, String>>>();
    _pendingStateMap = new HashMap<String, Map<Partition, Map<String, String>>>();
    _resourceStateModelMap = new HashMap<String, String>();
  }
  
  public void setResourceStateModelDef(String resourceName, String stateModelDefName)
  {
    _resourceStateModelMap.put(resourceName, stateModelDefName);
  }
  
  public String getResourceStateModelDef(String resourceName)
  {
    return _resourceStateModelMap.get(resourceName);
  }

  public void setCurrentState(String resourceName,
      Partition partition, String instanceName, String state)
  {
    if (!_currentStateMap.containsKey(resourceName))
    {
      _currentStateMap.put(resourceName,
          new HashMap<Partition, Map<String, String>>());
    }
    if (!_currentStateMap.get(resourceName).containsKey(partition))
    {
      _currentStateMap.get(resourceName).put(partition,
          new HashMap<String, String>());
    }
    _currentStateMap.get(resourceName).get(partition)
        .put(instanceName, state);
  }

  public void setPendingState(String resourceName,
      Partition partition, String instanceName, String state)
  {
    if (!_pendingStateMap.containsKey(resourceName))
    {
      _pendingStateMap.put(resourceName,
          new HashMap<Partition, Map<String, String>>());
    }
    if (!_pendingStateMap.get(resourceName).containsKey(partition))
    {
      _pendingStateMap.get(resourceName).put(partition,
          new HashMap<String, String>());
    }
    _pendingStateMap.get(resourceName).get(partition)
        .put(instanceName, state);
  }

  public String getCurrentState(String resourceName, Partition resource,
      String instanceName)
  {
    Map<Partition, Map<String, String>> map = _currentStateMap
        .get(resourceName);
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

  public String getPendingState(String resourceName, Partition resource,
      String instanceName)
  {
    Map<Partition, Map<String, String>> map = _pendingStateMap
        .get(resourceName);
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

  public Map<String, String> getCurrentStateMap(String resourceName,
      Partition resource)
  {
    Map<Partition, Map<String, String>> map = _currentStateMap
        .get(resourceName);
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
