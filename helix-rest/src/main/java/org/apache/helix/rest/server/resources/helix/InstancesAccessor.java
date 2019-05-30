package org.apache.helix.rest.server.resources.helix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.common.HelixDataAccessorWrapper;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.resources.exceptions.HelixHealthException;
import org.apache.helix.rest.server.service.ClusterService;
import org.apache.helix.rest.server.service.ClusterServiceImpl;
import org.apache.helix.rest.server.service.InstanceService;
import org.apache.helix.rest.server.service.InstanceServiceImpl;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/clusters/{clusterId}/instances")
public class InstancesAccessor extends AbstractHelixResource {
  private final static Logger _logger = LoggerFactory.getLogger(InstancesAccessor.class);
  // This type does not belongs to real HealthCheck failed reason. Also if we add this type
  // to HealthCheck enum, it could introduce more unnecessary check step since the InstanceServiceImpl
  // loops all the types to do corresponding checks.
  private final static String INSTANCE_NOT_EXIST = "Helix:INSTANCE_NOT_EXIST";
  public enum InstancesProperties {
    instances,
    online,
    disabled,
    selection_base,
    zone_order,
    customized_values,
    instance_stoppable_parallel,
    instance_not_stoppable_with_reasons
  }

  public enum InstanceHealthSelectionBase {
    instance_based,
    zone_based
  }

  @GET
  public Response getAllInstances(@PathParam("clusterId") String clusterId) {
    HelixDataAccessor accessor = getDataAccssor(clusterId);
    List<String> instances = accessor.getChildNames(accessor.keyBuilder().instanceConfigs());

    if (instances == null) {
      return notFound();
    }

    ObjectNode root = JsonNodeFactory.instance.objectNode();
    root.put(Properties.id.name(), JsonNodeFactory.instance.textNode(clusterId));

    ArrayNode instancesNode = root.putArray(InstancesAccessor.InstancesProperties.instances.name());
    instancesNode.addAll((ArrayNode) OBJECT_MAPPER.valueToTree(instances));
    ArrayNode onlineNode = root.putArray(InstancesAccessor.InstancesProperties.online.name());
    ArrayNode disabledNode = root.putArray(InstancesAccessor.InstancesProperties.disabled.name());

    List<String> liveInstances = accessor.getChildNames(accessor.keyBuilder().liveInstances());
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());

    for (String instanceName : instances) {
      InstanceConfig instanceConfig =
          accessor.getProperty(accessor.keyBuilder().instanceConfig(instanceName));
      if (instanceConfig != null) {
        if (!instanceConfig.getInstanceEnabled() || (clusterConfig.getDisabledInstances() != null
            && clusterConfig.getDisabledInstances().containsKey(instanceName))) {
          disabledNode.add(JsonNodeFactory.instance.textNode(instanceName));
        }

        if (liveInstances.contains(instanceName)){
          onlineNode.add(JsonNodeFactory.instance.textNode(instanceName));
        }
      }
    }

    return JSONRepresentation(root);
  }

  @POST
  public Response instancesOperations(@PathParam("clusterId") String clusterId,
      @QueryParam("command") String command, String content) {
    Command cmd;
    try {
      cmd = Command.valueOf(command);
    } catch (Exception e) {
      return badRequest("Invalid command : " + command);
    }

    HelixAdmin admin = getHelixAdmin();
    try {
      JsonNode node = null;
      if (content.length() != 0) {
        node = OBJECT_MAPPER.readTree(content);
      }
      if (node == null) {
        return badRequest("Invalid input for content : " + content);
      }
      List<String> enableInstances = OBJECT_MAPPER
          .readValue(node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      switch (cmd) {
      case enable:
        admin.enableInstance(clusterId, enableInstances, true);
        break;
      case disable:
        admin.enableInstance(clusterId, enableInstances, false);
        break;
      case stoppable:
        return getParallelStoppableInstances(clusterId, node);
      default:
        _logger.error("Unsupported command :" + command);
        return badRequest("Unsupported command :" + command);
      }
    } catch (HelixHealthException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      return serverError(e);
    } catch (Exception e) {
      _logger.error("Failed in updating instances : " + content, e);
      return badRequest(e.getMessage());
    }
    return OK();
  }

  private Response getParallelStoppableInstances(String clusterId, JsonNode node)
      throws IOException {
    try {
      // TODO: Process input data from the content
      InstancesAccessor.InstanceHealthSelectionBase selectionBase =
          InstancesAccessor.InstanceHealthSelectionBase.valueOf(
              node.get(InstancesAccessor.InstancesProperties.selection_base.name())
                  .getValueAsText());
      List<String> instances = OBJECT_MAPPER
          .readValue(node.get(InstancesAccessor.InstancesProperties.instances.name()).toString(),
              OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));

      List<String> orderOfZone = null;
      String customizedInput = null;
      if (node.get(InstancesAccessor.InstancesProperties.customized_values.name()) != null) {
        customizedInput = node.get(InstancesAccessor.InstancesProperties.customized_values.name()).getTextValue();
      }

      if (node.get(InstancesAccessor.InstancesProperties.zone_order.name()) != null) {
        orderOfZone = OBJECT_MAPPER
            .readValue(node.get(InstancesAccessor.InstancesProperties.zone_order.name()).toString(),
                OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, String.class));
      }

      // Prepare output result
      ObjectNode result = JsonNodeFactory.instance.objectNode();
      ArrayNode stoppableInstances =
          result.putArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
      ObjectNode failedStoppableInstances = result.putObject(
          InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
      InstanceService instanceService =
          new InstanceServiceImpl(new HelixDataAccessorWrapper((ZKHelixDataAccessor) getDataAccssor(clusterId)), getConfigAccessor());
      ClusterService clusterService =
          new ClusterServiceImpl(getDataAccssor(clusterId), getConfigAccessor());
      ClusterTopology clusterTopology = clusterService.getClusterTopology(clusterId);
      switch (selectionBase) {
      case zone_based:
        List<String> zoneBasedInstance =
            getZoneBasedInstances(instances, orderOfZone, clusterTopology.toZoneMapping());
        for (String instance : zoneBasedInstance) {
          StoppableCheck stoppableCheckResult =
              instanceService.getInstanceStoppableCheck(clusterId, instance, customizedInput);
          if (!stoppableCheckResult.isStoppable()) {
            ArrayNode failedReasonsNode = failedStoppableInstances.putArray(instance);
            for (String failedReason : stoppableCheckResult.getFailedChecks()) {
              failedReasonsNode.add(JsonNodeFactory.instance.textNode(failedReason));
            }
          } else {
            stoppableInstances.add(instance);
          }
        }

        // Adding following logic to check whether instances exist or not. An instance exist could be
        // checking following scenario:
        // 1. Instance got dropped. (InstanceConfig is gone.)
        // 2. Instance name has typo.

        // If we dont add this check, the instance, which does not exist, will be disappeared from
        // result since Helix skips instances for instances not in the selected zone. User may get
        // confused with the output.
        Set<String> nonSelectedInstances = new HashSet<>(instances);
        nonSelectedInstances.removeAll(clusterTopology.getAllInstances());
        for (String nonSelectedInstance : nonSelectedInstances) {
          ArrayNode failedReasonsNode = failedStoppableInstances.putArray(nonSelectedInstance);
          failedReasonsNode.add(JsonNodeFactory.instance.textNode(INSTANCE_NOT_EXIST));
        }

        break;
      case instance_based:
      default:
        throw new NotImplementedException("instance_based selection is not supported yet!");
      }
      return JSONRepresentation(result);
    } catch (HelixException e) {
      _logger
          .error(String.format("Current cluster %s has issue with health checks!", clusterId), e);
      throw new HelixHealthException(e);
    } catch (Exception e) {
      _logger.error(String.format(
          "Failed to get parallel stoppable instances for cluster %s with a HelixException!",
          clusterId), e);
      throw e;
    }
  }

  /**
   * Get instances belongs to the first zone. If the zone is already empty, Helix will iterate zones
   * by order until find the zone contains instances.
   *
   * The order of zones can directly come from user input. If user did not specify it, Helix will order
   * zones with alphabetical order.
   *
   * @param instances
   * @param orderedZones
   * @return
   */
  private List<String> getZoneBasedInstances(List<String> instances, List<String> orderedZones,
      Map<String, Set<String>> zoneMapping) {

    if (orderedZones == null) {
      orderedZones = new ArrayList<>(zoneMapping.keySet());
    }
    Collections.sort(orderedZones);
    if (orderedZones.isEmpty()) {
      return orderedZones;
    }

    Set<String> instanceSet = null;
    for (String zone : orderedZones) {
      instanceSet = new TreeSet<>(instances);
      Set<String> currentZoneInstanceSet = new HashSet<>(zoneMapping.get(zone));
      instanceSet.retainAll(currentZoneInstanceSet);
      if (instanceSet.size() > 0) {
        return new ArrayList<>(instanceSet);
      }
    }

    return Collections.EMPTY_LIST;
  }
}
