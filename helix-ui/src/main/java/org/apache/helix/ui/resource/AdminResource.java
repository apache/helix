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

import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.ui.api.ClusterConnection;
import org.apache.helix.ui.util.ClientCache;
import org.apache.helix.ui.util.DataCache;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/admin")
public class AdminResource {
  private final ClientCache clientCache;
  private final DataCache dataCache;

  public AdminResource(ClientCache clientCache, DataCache dataCache) {
    this.clientCache = clientCache;
    this.dataCache = dataCache;
  }

  @Path("/{zkAddress}/{clusterName}")
  @POST
  public Response addCluster(@PathParam("zkAddress") String zkAddress,
                             @PathParam("clusterName") String clusterName) throws Exception {
    dataCache.invalidate();

    ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

    if (clusterSetup.getClusterManagementTool().getClusters().contains(clusterName)) {
      return Response.status(Response.Status.CONFLICT).build();
    }

    clusterSetup.addCluster(clusterName, false);

    dataCache.invalidate();

    return Response.ok().build();
  }

  @Path("/{zkAddress}/{clusterName}")
  @DELETE
  public Response dropCluster(@PathParam("zkAddress") String zkAddress,
                              @PathParam("clusterName") String clusterName) throws Exception {
    dataCache.invalidate();

    ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

    if (!clusterSetup.getClusterManagementTool().getClusters().contains(clusterName)) {
      throw new NotFoundException();
    }

    clusterSetup.getClusterManagementTool().dropCluster(clusterName);

    return Response.noContent().build();
  }

  @Path("/{zkAddress}/{clusterName}/instances/{instanceName}")
  @POST
  public Response addInstance(@PathParam("zkAddress") String zkAddress,
                              @PathParam("clusterName") String clusterName,
                              @PathParam("instanceName") String instanceName,
                              @QueryParam("disable") boolean disable,
                              @QueryParam("failIfNoInstance") boolean failIfNoInstance) throws Exception {
    dataCache.invalidate();

    ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

    List<String> instances = clusterSetup.getClusterManagementTool().getInstancesInCluster(clusterName);

    Response response;

    if (instances.contains(instanceName)) {
      response = Response.notModified().build();
    } else if (failIfNoInstance) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    } else {
      clusterSetup.addInstanceToCluster(clusterName, instanceName);
      response = Response.ok().build();
    }

    clusterSetup.getClusterManagementTool().enableInstance(clusterName, instanceName, !disable);

    return response;
  }

  @Path("/{zkAddress}/{clusterName}/instances/{instanceName}")
  @DELETE
  public Response dropInstance(@PathParam("zkAddress") String zkAddress,
                               @PathParam("clusterName") String clusterName,
                               @PathParam("instanceName") String instanceName) throws Exception {
    dataCache.invalidate();

    ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

    InstanceConfig instanceConfig
            = clusterSetup.getClusterManagementTool().getInstanceConfig(clusterName, instanceName);

    if (instanceConfig == null) {
      throw new NotFoundException();
    } else if (instanceConfig.getInstanceEnabled()) {
      return Response.status(Response.Status.BAD_REQUEST)
              .header("X-Error-Message", "Cannot drop instance that is enabled")
              .build();
    }

    clusterSetup.dropInstanceFromCluster(clusterName, instanceName);

    return Response.noContent().build();
  }

  @Path("/{zkAddress}/{clusterName}/resources/{resourceName}/{partitions}/{replicas}")
  @POST
  public Response addResource(@PathParam("zkAddress") String zkAddress,
                              @PathParam("clusterName") String clusterName,
                              @PathParam("resourceName") String resourceName,
                              @PathParam("partitions") int partitions,
                              @PathParam("replicas") String replicas,
                              @QueryParam("rebalance") boolean rebalance,
                              @QueryParam("stateModel") String stateModel,
                              @QueryParam("rebalanceMode") String rebalanceMode) throws Exception {
    dataCache.invalidate();

    ClusterConnection conn = clientCache.get(zkAddress);
    ClusterSetup clusterSetup = conn.getClusterSetup();

    if (!ZKUtil.isClusterSetup(clusterName, conn.getZkClient())) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    IdealState existingIdealState
            = clusterSetup.getClusterManagementTool().getResourceIdealState(clusterName, resourceName);

    IdealState idealState = new IdealState(resourceName);
    idealState.setNumPartitions(partitions);
    idealState.setReplicas(replicas);
    idealState.setStateModelDefRef(stateModel == null
            ? "OnlineOffline" : stateModel);
    idealState.setRebalanceMode(rebalanceMode == null
            ? IdealState.RebalanceMode.FULL_AUTO : IdealState.RebalanceMode.valueOf(rebalanceMode));

    Response response;

    if (existingIdealState == null) {
      clusterSetup.getClusterManagementTool().addResource(clusterName, resourceName, idealState);
      response = Response.ok().build();
    } else if (!existingIdealState.equals(idealState)) {
      return Response.status(Response.Status.CONFLICT).build();
    } else {
      response = Response.notModified().build();
    }

    if (rebalance) { // TODO this will break if replicas is not integer (e.g. N)
      clusterSetup.rebalanceResource(clusterName, resourceName, Integer.valueOf(replicas));
    }

    return response;
  }

  @Path("/{zkAddress}/{clusterName}/resources/{resourceName}")
  @DELETE
  public Response dropResource(@PathParam("zkAddress") String zkAddress,
                               @PathParam("clusterName") String clusterName,
                               @PathParam("resourceName") String resourceName) throws Exception {
    dataCache.invalidate();

    ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

    List<String> resources = clusterSetup.getClusterManagementTool().getResourcesInCluster(clusterName);
    if (!resources.contains(resourceName)) {
      throw new NotFoundException();
    }

    clusterSetup.dropResourceFromCluster(clusterName, resourceName);

    return Response.noContent().build();
  }
}
