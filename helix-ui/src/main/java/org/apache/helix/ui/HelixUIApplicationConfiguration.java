package org.apache.helix.ui;

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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.Configuration;

import javax.validation.constraints.NotNull;
import java.util.Set;

public class HelixUIApplicationConfiguration extends Configuration {
  @NotNull
  private ImmutableMap<String, ImmutableMap<String, String>> viewRendererConfiguration = ImmutableMap.of();

  private boolean adminMode = false;

  private Set<String> zkAddresses;

  @JsonProperty("viewRendererConfiguration")
  public ImmutableMap<String, ImmutableMap<String, String>> getViewRendererConfiguration() {
    return viewRendererConfiguration;
  }

  public void setAdminMode(boolean adminMode) {
    this.adminMode = adminMode;
  }

  public boolean isAdminMode() {
    return adminMode;
  }

  public void setZkAddresses(Set<String> zkAddresses) {
    this.zkAddresses = zkAddresses;
  }

  public Set<String> getZkAddresses() {
    return zkAddresses;
  }
}
