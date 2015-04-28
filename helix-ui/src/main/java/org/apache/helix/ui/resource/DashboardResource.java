package org.apache.helix.ui.resource;

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

import com.google.common.collect.ImmutableList;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.ui.api.*;
import org.apache.helix.ui.util.ClientCache;
import org.apache.helix.ui.util.DataCache;
import org.apache.helix.ui.view.ClusterView;
import org.apache.helix.ui.view.LandingView;
import org.apache.helix.ui.view.ResourceView;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.*;

@Path("/")
@Produces(MediaType.TEXT_HTML)
public class DashboardResource {
  private static final List<String> REBALANCE_MODES = ImmutableList.of(
          IdealState.RebalanceMode.SEMI_AUTO.toString(),
          IdealState.RebalanceMode.FULL_AUTO.toString(),
          IdealState.RebalanceMode.CUSTOMIZED.toString(),
          IdealState.RebalanceMode.USER_DEFINED.toString(),
          IdealState.RebalanceMode.TASK.toString());

  private final boolean adminMode;
  private final ClientCache clientCache;
  private final DataCache dataCache;

  public DashboardResource(ClientCache clientCache,
                           DataCache dataCache,
                           boolean adminMode) {
    this.clientCache = clientCache;
    this.dataCache = dataCache;
    this.adminMode = adminMode;
  }

  @GET
  public Response getRoot() {
    return Response.seeOther(URI.create("/dashboard")).build();
  }

  @GET
  @Path("/dashboard")
  public LandingView getLandingView() {
    return new LandingView();
  }

  @GET
  @Path("/dashboard/{zkAddress}")
  public ClusterView getClusterView(@PathParam("zkAddress") String zkAddress) throws Exception {
    clientCache.get(zkAddress); // n.b. will validate
    return getClusterView(zkAddress, null);
  }

  @GET
  @Path("/dashboard/{zkAddress}/{cluster}")
  public ClusterView getClusterView(
          @PathParam("zkAddress") String zkAddress,
          @PathParam("cluster") String cluster) throws Exception {
    ClusterConnection clusterConnection = clientCache.get(zkAddress);

    // All clusters
    List<String> clusters = dataCache.getClusterCache().get(zkAddress);

    // The active cluster
    String activeCluster = cluster == null ? clusters.get(0) : cluster;
    ClusterSpec clusterSpec = new ClusterSpec(zkAddress, activeCluster);

    // Check it
    if (!ZKUtil.isClusterSetup(activeCluster, clusterConnection.getZkClient())) {
      return new ClusterView(adminMode, zkAddress, clusters, false, activeCluster, null, null, null, null, null);
    }

    // Resources in the active cluster
    List<String> activeClusterResources = dataCache.getResourceCache().get(clusterSpec);

    // Instances in active cluster
    List<InstanceSpec> instanceSpecs = dataCache.getInstanceCache().get(clusterSpec);

    // State models in active cluster
    List<String> stateModels
            = clusterConnection.getClusterSetup().getClusterManagementTool().getStateModelDefs(activeCluster);

    // Config table
    List<ConfigTableRow> configTable = dataCache.getConfigCache().get(clusterSpec);

    return new ClusterView(
            adminMode,
            zkAddress,
            clusters,
            true,
            activeCluster,
            activeClusterResources,
            instanceSpecs,
            configTable,
            stateModels,
            REBALANCE_MODES);
  }

  @GET
  @Path("/dashboard/{zkAddress}/{cluster}/{resource}")
  public ResourceView getResourceView(
          @PathParam("zkAddress") String zkAddress,
          @PathParam("cluster") String cluster,
          @PathParam("resource") String resource) throws Exception {
    ClusterConnection clusterConnection = clientCache.get(zkAddress);

    // All clusters
    List<String> clusters = dataCache.getClusterCache().get(zkAddress);

    // The active cluster
    String activeCluster = cluster == null ? clusters.get(0) : cluster;
    ClusterSpec clusterSpec = new ClusterSpec(zkAddress, activeCluster);

    // Check it
    if (!ZKUtil.isClusterSetup(activeCluster, clusterConnection.getZkClient())) {
      return new ResourceView(
              adminMode, zkAddress, clusters, false, activeCluster, null, null, null, null, null, null, null);
    }

    // Resources in the active cluster
    List<String> activeClusterResources = dataCache.getResourceCache().get(clusterSpec);
    if (!activeClusterResources.contains(resource)) {
      throw new NotFoundException("No resource " + resource + " in " + activeCluster);
    }

    // Instances in active cluster
    List<InstanceSpec> instanceSpecs = dataCache.getInstanceCache().get(clusterSpec);
    Map<String, InstanceSpec> instanceSpecMap = new HashMap<String, InstanceSpec>(instanceSpecs.size());
    for (InstanceSpec instanceSpec : instanceSpecs) {
      instanceSpecMap.put(instanceSpec.getInstanceName(), instanceSpec);
    }

    // Resource state
    IdealState idealState
            = clusterConnection.getClusterSetup().getClusterManagementTool().getResourceIdealState(cluster, resource);
    ExternalView externalView
            = clusterConnection.getClusterSetup().getClusterManagementTool().getResourceExternalView(cluster, resource);
    ResourceStateSpec resourceStateSpec
            = new ResourceStateSpec(resource, idealState, externalView, instanceSpecMap);
    List<ResourceStateTableRow> resourceStateTable
            = resourceStateSpec.getResourceStateTable();

    // Resource config
    List<ConfigTableRow> configTable = dataCache.getResourceConfigCache().get(new ResourceSpec(zkAddress, activeCluster, resource));

    // Resource instances
    Set<String> resourceInstances = new HashSet<String>();
    for (ResourceStateTableRow row : resourceStateTable) {
      resourceInstances.add(row.getInstanceName());
    }

    return new ResourceView(
            adminMode,
            zkAddress,
            clusters,
            true,
            activeCluster,
            activeClusterResources,
            resource,
            resourceStateTable,
            resourceInstances,
            configTable,
            IdealStateSpec.fromIdealState(idealState),
            instanceSpecs);
  }
}
