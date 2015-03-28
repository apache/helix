package org.apache.helix.ui.resource;

import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.ui.api.*;
import org.apache.helix.ui.util.ClientCache;
import org.apache.helix.ui.util.DataCache;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Path("/visualizer")
public class VisualizerResource {
    private final ClientCache clientCache;
    private final DataCache dataCache;

    public VisualizerResource(ClientCache clientCache, DataCache dataCache) {
        this.clientCache = clientCache;
        this.dataCache = dataCache;
    }

    @GET
    @Path("/{zkAddress}/{clusterName}/{resourceName}")
    @Produces(MediaType.APPLICATION_JSON)
    public D3ResourceCoCentricCircle getD3HelixResource(
            @PathParam("zkAddress") String zkAddress,
            @PathParam("clusterName") String clusterName,
            @PathParam("resourceName") String resourceName) throws Exception {
        ClusterSetup clusterSetup = clientCache.get(zkAddress).getClusterSetup();

        IdealState idealState
                = clusterSetup.getClusterManagementTool().getResourceIdealState(clusterName, resourceName);
        ExternalView externalView
                = clusterSetup.getClusterManagementTool().getResourceExternalView(clusterName, resourceName);
        if (idealState == null) {
            throw new NotFoundException("No resource ideal state for " + resourceName);
        }

        // Instances in active cluster
        List<InstanceSpec> instanceSpecs = dataCache.getInstanceCache().get(new ClusterSpec(zkAddress, clusterName));
        Map<String, InstanceSpec> instanceSpecMap = new HashMap<String, InstanceSpec>(instanceSpecs.size());
        for (InstanceSpec instanceSpec : instanceSpecs) {
            instanceSpecMap.put(instanceSpec.getInstanceName(), instanceSpec);
        }

        return D3ResourceCoCentricCircle.fromResourceStateSpec(
                new ResourceStateSpec(resourceName, idealState, externalView, instanceSpecMap));
    }
}
