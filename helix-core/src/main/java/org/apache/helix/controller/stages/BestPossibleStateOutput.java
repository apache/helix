package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.model.Partition;

/**
 * Output for BestPossibleStateCalStage.
 */
public class BestPossibleStateOutput extends ResourcesStateMap {
  /* resource -> partition -> preference list */
  private Map<String, Map<String, List<String>>> _preferenceLists;
  /**
   * Deprecated, use getResourceStatesMap instead.
   *
   * @param resourceName
   * @return
   */
  // TODO: remove this.
  @Deprecated
  public Map<Partition, Map<String, String>> getResourceMap(String resourceName) {
    PartitionStateMap map = _resourceStateMap.get(resourceName);
    if (map != null) {
      return map.getStateMap();
    }
    return Collections.emptyMap();
  }

  /**
   * Deprecated, use getResourceStatesMap instead.
   *
   * @return
   */
  // TODO: remove this.
  @Deprecated public Map<String, Map<Partition, Map<String, String>>> getStateMap() {
    Map<String, Map<Partition, Map<String, String>>> stateMap =
        new HashMap<String, Map<Partition, Map<String, String>>>();
    for (Map.Entry<String, PartitionStateMap> e : _resourceStateMap.entrySet()) {
      stateMap.put(e.getKey(), e.getValue().getStateMap());
    }
    return stateMap;
  }

  public Map<String, Map<String, List<String>>> getPreferenceLists() {
    return _preferenceLists;
  }

  public Map<String, List<String>> getPreferenceLists(String resource) {
    if (_preferenceLists != null && _preferenceLists.containsKey(resource)) {
      return _preferenceLists.get(resource);
    }

    return null;
  }

  public List<String> getPreferenceList(String resource, String partition) {
    if (_preferenceLists != null && _preferenceLists.containsKey(resource) && _preferenceLists
        .get(resource).containsKey(partition)) {
      return _preferenceLists.get(resource).get(partition);
    }

    return null;
  }

  public void setPreferenceList(String resource, String partition, List<String> list) {
    if (_preferenceLists == null) {
      _preferenceLists = new HashMap<String, Map<String, List<String>>>();
    }
    if (!_preferenceLists.containsKey(resource)) {
      _preferenceLists.put(resource, new HashMap<String, List<String>>());
    }
    _preferenceLists.get(resource).put(partition, list);
  }

  public void setPreferenceLists(String resource,
      Map<String, List<String>> resourcePreferenceLists) {
    if (_preferenceLists == null) {
      _preferenceLists = new HashMap<String, Map<String, List<String>>>();
    }
    _preferenceLists.put(resource, resourcePreferenceLists);
  }
}
