package org.apache.helix.ui.view;

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

import io.dropwizard.views.View;
import org.apache.helix.ui.api.ConfigTableRow;
import org.apache.helix.ui.api.InstanceSpec;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;

public class ClusterView extends View {
  private final boolean adminMode;
  private final String zkAddress;
  private final List<String> clusters;
  private final boolean activeValid;
  private final String activeCluster;
  private final List<String> activeClusterResources;
  private final List<InstanceSpec> instanceSpecs;
  private final List<ConfigTableRow> configTable;
  private final List<String> stateModels;
  private final List<String> rebalanceModes;

  public ClusterView(boolean adminMode,
                     String zkAddress,
                     List<String> clusters,
                     boolean activeValid,
                     String activeCluster,
                     List<String> activeClusterResources,
                     List<InstanceSpec> instanceSpecs,
                     List<ConfigTableRow> configTable,
                     List<String> stateModels,
                     List<String> rebalanceModes) {
    super("cluster-view.ftl");
    this.adminMode = adminMode;
    this.zkAddress = zkAddress;
    this.clusters = clusters;
    this.activeValid = activeValid;
    this.activeCluster = activeCluster;
    this.activeClusterResources = activeClusterResources;
    this.instanceSpecs = instanceSpecs;
    this.configTable = configTable;
    this.stateModels = stateModels;
    this.rebalanceModes = rebalanceModes;
  }

  public boolean isAdminMode() {
    return adminMode;
  }

  public String getZkAddress() throws IOException {
    return URLEncoder.encode(zkAddress, "UTF-8");
  }

  public List<String> getClusters() {
    return clusters;
  }

  public boolean isActiveValid() {
    return activeValid;
  }

  public String getActiveCluster() {
    return activeCluster;
  }

  public List<String> getActiveClusterResources() {
    return activeClusterResources;
  }

  public List<InstanceSpec> getInstanceSpecs() {
    return instanceSpecs;
  }

  public List<ConfigTableRow> getConfigTable() {
    return configTable;
  }

  public List<String> getStateModels() {
    return stateModels;
  }

  public List<String> getRebalanceModes() {
    return rebalanceModes;
  }
}
