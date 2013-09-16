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
import java.util.Map;

import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.State;
import org.apache.helix.model.Partition;

@Deprecated
public class BestPossibleStateOutput {
  // resource->partition->instance->state
  Map<String, Map<Partition, Map<String, String>>> _dataMap;

  public BestPossibleStateOutput() {
    _dataMap = new HashMap<String, Map<Partition, Map<String, String>>>();
  }

  public void setState(String resourceName, Partition resource,
      Map<String, String> bestInstanceStateMappingForResource) {
    if (!_dataMap.containsKey(resourceName)) {
      _dataMap.put(resourceName, new HashMap<Partition, Map<String, String>>());
    }
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    map.put(resource, bestInstanceStateMappingForResource);
  }

  public void setParticipantStateMap(String resourceName, Partition partition,
      Map<ParticipantId, State> bestInstanceStateMappingForResource) {
    Map<String, String> rawStateMap = new HashMap<String, String>();
    for (ParticipantId participantId : bestInstanceStateMappingForResource.keySet()) {
      rawStateMap.put(participantId.stringify(),
          bestInstanceStateMappingForResource.get(participantId).toString());
    }
    setState(resourceName, partition, rawStateMap);
  }

  public Map<String, String> getInstanceStateMap(String resourceName, Partition resource) {
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    if (map != null) {
      return map.get(resource);
    }
    return Collections.emptyMap();
  }

  public Map<Partition, Map<String, String>> getResourceMap(String resourceName) {
    Map<Partition, Map<String, String>> map = _dataMap.get(resourceName);
    if (map != null) {
      return map;
    }
    return Collections.emptyMap();
  }

  public Map<String, Map<Partition, Map<String, String>>> getStateMap() {
    return _dataMap;
  }

  @Override
  public String toString() {
    return _dataMap.toString();
  }
}
