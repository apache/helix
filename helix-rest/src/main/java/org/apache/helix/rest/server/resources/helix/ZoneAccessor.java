package org.apache.helix.rest.server.resources.helix;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.rest.server.filters.NamespaceAuth;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ClusterAuth
@Path("/clusters/{clusterId}/zones")
public class ZoneAccessor extends AbstractHelixResource {
  private final static Logger LOG = LoggerFactory.getLogger(ZoneAccessor.class);

  @NamespaceAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  public Response getZones(@PathParam("clusterId") String clusterId) {
    ClusterService clusterService =
        new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
    ClusterTopology clusterTopology = clusterService.getClusterTopology(clusterId);
    Set<String> zones = clusterTopology.getZones().stream()
        .map(ClusterTopology.Zone::getId)
        .collect(Collectors.toSet());
    return JSONRepresentation(zones);
  }

  @NamespaceAuth
  @ResponseMetered(name = HttpConstants.READ_REQUEST)
  @Timed(name = HttpConstants.READ_REQUEST)
  @GET
  @Path("disabledZones")
  public Response getDisabledZones(@PathParam("clusterId") String clusterId) {
    ConfigAccessor accessor = getConfigAccessor();
    ClusterConfig clusterConfig = accessor.getClusterConfig(clusterId);
    return JSONRepresentation(clusterConfig.getDisabledZones());
  }

  @ClusterAuth
  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  @Path("{zoneId}")
  public Response update(@PathParam("clusterId") String clusterId, @PathParam("zoneId") String zoneId,
      @QueryParam("command") String commandStr) {
    Command command;
    try {
      command = getCommand(commandStr);
    } catch (HelixException ex) {
      return badRequest(ex.getMessage());
    }
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    BaseDataAccessor<ZNRecord> baseDataAccessor = accessor.getBaseDataAccessor();
    String path = PropertyPathBuilder.clusterConfig(clusterId);

    baseDataAccessor.update(path, currentData -> {
      if (currentData == null) {
        throw new HelixException("Cluster: " + clusterId + ": cluster config is null");
      }
      ClusterConfig clusterConfig = new ClusterConfig(currentData);
      switch (command) {
        case enable:
          LOG.info("Enable zone {} in cluster {}.", zoneId, clusterId);
          clusterConfig.removeDisabledZone(zoneId);
          break;
        case disable:
          LOG.info("Disable zone {} in cluster {}.", zoneId, clusterId);
          clusterConfig.addDisabledZone(zoneId);
          break;
        default:
          throw new HelixException("Illegal command: " + commandStr);
      }
      return clusterConfig.getRecord();
    }, AccessOption.PERSISTENT);

    return OK();
  }
}
