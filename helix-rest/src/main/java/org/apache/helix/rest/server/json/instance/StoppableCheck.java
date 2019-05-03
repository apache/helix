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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

public class StoppableCheck {
  // Category to differentiate which step the check fails
  public enum Category {
    HELIX_OWN_CHECK("Helix:"),
    CUSTOM_INSTANCE_CHECK("CustomInstance:"),
    CUSTOM_PARTITION_CHECK("CustomPartition:");

    String prefix;

    Category(String prefix) {
      this.prefix = prefix;
    }
  }

  @JsonProperty("stoppable")
  private boolean isStoppable;
  // The list of failed checks should be sorted to make test consistent pass
  @JsonProperty("failedChecks")
  private List<String> failedChecks;

  public StoppableCheck(boolean isStoppable, List<String> failedChecks, Category category) {
    this.isStoppable = isStoppable;
    this.failedChecks = failedChecks.stream()
        .sorted()
        .map(checkName -> appendPrefix(checkName, category))
        .collect(Collectors.toList());
  }

  public StoppableCheck(Map<String, Boolean> checks, Category category) {
    this.failedChecks = Maps.filterValues(checks, Boolean.FALSE::equals).keySet()
        .stream()
        .sorted()
        .map(checkName -> appendPrefix(checkName, category))
        .collect(Collectors.toList());
    this.isStoppable = this.failedChecks.isEmpty();
  }

  private String appendPrefix(String checkName, Category category) {
    return category.prefix + checkName;
  }

  public boolean isStoppable() {
    return isStoppable;
  }

  public List<String> getFailedChecks() {
    return failedChecks;
  }
}
