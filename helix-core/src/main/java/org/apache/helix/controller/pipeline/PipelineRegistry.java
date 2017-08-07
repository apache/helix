package org.apache.helix.controller.pipeline;

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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.stages.ClusterEventType;

public class PipelineRegistry {
  Map<ClusterEventType, List<Pipeline>> _map;

  public PipelineRegistry() {
    _map = new HashMap<>();
  }

  public void register(ClusterEventType eventType, Pipeline... pipelines) {
    if (!_map.containsKey(eventType)) {
      _map.put(eventType, new ArrayList<Pipeline>());
    }
    List<Pipeline> list = _map.get(eventType);
    for (Pipeline pipeline : pipelines) {
      list.add(pipeline);
    }
  }

  public List<Pipeline> getPipelinesForEvent(ClusterEventType eventType) {
    if (_map.containsKey(eventType)) {
      return _map.get(eventType);
    }
    return Collections.emptyList();
  }
}
