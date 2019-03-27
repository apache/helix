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
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class InstanceInfo {
  private static final Logger _logger = LoggerFactory.getLogger(InstanceInfo.class);

  @JsonProperty("id")
  private final String id;
  @JsonProperty("liveInstance")
  private final ZNRecord liveInstance;
  @JsonProperty("config")
  private final ZNRecord instanceConfig;
  @JsonProperty("partitions")
  private final List<String> partitions;
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @JsonProperty("resources")
  private final List<String> resources;
  @JsonProperty("health")
  private final boolean isHealth;
  @JsonProperty("failedHealthChecks")
  private final List<String> failedHealthChecks;

  private InstanceInfo(Builder builder) {
    id = builder.id;
    liveInstance = builder.liveInstance;
    instanceConfig = builder.instanceConfig;
    partitions = builder.partitions;
    resources = builder.resources;
    isHealth = builder.isHealth;
    failedHealthChecks = builder.failedHealthChecks;
  }

  public static final class Builder {
    private String id;
    private ZNRecord liveInstance;
    private ZNRecord instanceConfig;
    private List<String> partitions;
    private List<String> resources;
    private boolean isHealth;
    private List<String> failedHealthChecks;

    public Builder(String id) {
      this.id = id;
    }

    public Builder liveInstance(ZNRecord liveInstance) {
      this.liveInstance = liveInstance;
      return this;
    }

    public Builder instanceConfig(ZNRecord instanceConfig) {
      this.instanceConfig = instanceConfig;
      return this;
    }

    public Builder partitions(List<String> partitions) {
      this.partitions = partitions;
      return this;
    }

    public Builder resources(List<String> resources) {
      this.resources = resources;
      return this;
    }

    public Builder healthStatus(Map<String, Boolean> healthChecks) {
      this.failedHealthChecks = new ArrayList<>();
      for (String healthCheck : healthChecks.keySet()) {
        if (!healthChecks.get(healthCheck)) {
          _logger.warn("Health Check {} failed", healthCheck);
          this.failedHealthChecks.add(healthCheck);
        }
      }
      this.isHealth = this.failedHealthChecks.isEmpty();
      return this;
    }

    public InstanceInfo build() {
      return new InstanceInfo(this);
    }
  }
}
