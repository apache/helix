package org.apache.helix.rest.server.json.cluster;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;


public class ClusterInfo {
  @JsonProperty("id")
  private final String id;
  @JsonProperty("controller")
  private final String controller;
  @JsonProperty("paused")
  private final boolean paused;
  @JsonProperty("maintenance")
  private final boolean maintenance;
  @JsonProperty("resources")
  private final List<String> idealStates;
  @JsonProperty("instances")
  private final List<String> instances;
  @JsonProperty("liveInstances")
  private final List<String> liveInstances;

  private ClusterInfo(Builder builder) {
    id = builder.id;
    controller = builder.controller;
    paused = builder.paused;
    maintenance = builder.maintenance;
    idealStates = builder.idealStates;
    instances = builder.instances;
    liveInstances = builder.liveInstances;
  }

  public static final class Builder {
    private String id;
    private String controller;
    private boolean paused;
    private boolean maintenance;
    private List<String> idealStates;
    private List<String> instances;
    private List<String> liveInstances;

    public Builder(String id) {
      this.id = id;
    }

    public Builder controller(String controller) {
      this.controller = controller;
      return this;
    }

    public Builder paused(boolean paused) {
      this.paused = paused;
      return this;
    }

    public Builder maintenance(boolean maintenance) {
      this.maintenance = maintenance;
      return this;
    }

    public Builder idealStates(List<String> idealStates) {
      this.idealStates = idealStates;
      return this;
    }

    public Builder instances(List<String> instances) {
      this.instances = instances;
      return this;
    }

    public Builder liveInstances(List<String> liveInstances) {
      this.liveInstances = liveInstances;
      return this;
    }

    public ClusterInfo build() {
      return new ClusterInfo(this);
    }
  }
}
