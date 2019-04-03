package org.apache.helix.rest.server.json.instance;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public class StoppableCheck {
  private static final String HELIX_CHECK_PREFIX = "Helix:";
  private static final String CUSTOM_CHECK_PREFIX = "Custom:";

  @JsonProperty("stoppable")
  private boolean isStoppable;
  @JsonProperty("failedChecks")
  private List<String> failedChecks;

  public StoppableCheck(boolean isStoppable, List<String> failedChecks) {
    this.isStoppable = isStoppable;
    // sort the failed checks in order so that tests can always pass
    Collections.sort(failedChecks);
    this.failedChecks = failedChecks;
  }

  public static StoppableCheck mergeStoppableChecks(Map<String, Boolean> helixChecks, Map<String, Boolean> customChecks) {
    Map<String, Boolean> mergedResult = ImmutableMap.<String, Boolean>builder()
        .putAll(appendPrefix(helixChecks, HELIX_CHECK_PREFIX))
        .putAll(appendPrefix(customChecks, CUSTOM_CHECK_PREFIX))
        .build();

    List<String> failedChecks = new ArrayList<>();
    for (Map.Entry<String, Boolean> entry : mergedResult.entrySet()) {
      if (!entry.getValue()) {
        failedChecks.add(entry.getKey());
      }
    }

    return new StoppableCheck(failedChecks.isEmpty(), failedChecks);
  }

  private static Map<String, Boolean> appendPrefix(Map<String, Boolean> checks, String prefix) {
    Map<String, Boolean> result = new HashMap<>();
    for (Map.Entry<String, Boolean> entry : checks.entrySet()) {
      result.put(prefix + entry.getKey(), entry.getValue());
    }

    return result;
  }

  public boolean isStoppable() {
    return isStoppable;
  }

  public List<String> getFailedChecks() {
    return failedChecks;
  }
}
