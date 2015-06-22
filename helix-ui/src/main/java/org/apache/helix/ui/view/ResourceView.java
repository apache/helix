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
import org.apache.helix.ui.api.IdealStateSpec;
import org.apache.helix.ui.api.InstanceSpec;
import org.apache.helix.ui.api.ResourceStateTableRow;

import java.io.IOException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ResourceView extends View {
  private final boolean adminMode;
  private final String zkAddress;
  private final List<String> clusters;
  private final boolean activeValid;
  private final String activeCluster;
  private final List<String> activeClusterResources;
  private final String activeResource;
  private final List<ResourceStateTableRow> resourceStateTable;
  private final List<ConfigTableRow> configTable;
  private final IdealStateSpec idealStateSpec;
  private final List<InstanceSpec> instanceSpecs;

  public ResourceView(boolean adminMode,
                      String zkAddress,
                      List<String> clusters,
                      boolean activeValid,
                      String activeCluster,
                      List<String> activeClusterResources,
                      String activeResource,
                      List<ResourceStateTableRow> resourceStateTable,
                      Set<String> resourceInstances,
                      List<ConfigTableRow> configTable,
                      IdealStateSpec idealStateSpec,
                      List<InstanceSpec> instanceSpecs) {
    super("resource-view.ftl");
    this.adminMode = adminMode;
    this.zkAddress = zkAddress;
    this.clusters = clusters;
    this.activeValid = activeValid;
    this.activeCluster = activeCluster;
    this.activeClusterResources = activeClusterResources;
    this.activeResource = activeResource;
    this.resourceStateTable = resourceStateTable;
    this.configTable = configTable;
    this.idealStateSpec = idealStateSpec;
    this.instanceSpecs = new ArrayList<InstanceSpec>();

    for (InstanceSpec instanceSpec : instanceSpecs) {
      if (resourceInstances.contains(instanceSpec.getInstanceName())) {
        this.instanceSpecs.add(instanceSpec);
      }
    }
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

  public String getActiveResource() {
    return activeResource;
  }

  public List<ResourceStateTableRow> getResourceStateTable() {
    return resourceStateTable;
  }

  public List<ConfigTableRow> getConfigTable() {
    return configTable;
  }

  public IdealStateSpec getIdealStateSpec() {
    return idealStateSpec;
  }
}
