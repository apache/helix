package org.apache.helix.rest.server.resources.helix;

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

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.controller.rebalancer.strategy.AutoRebalanceStrategy;
import org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.rest.server.filters.ClusterAuth;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@ClusterAuth
@Path("/clusters/{clusterId}/partitionAssignment")
public class ResourceAssignmentOptimizerAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(
      org.apache.helix.rest.server.resources.helix.ResourceAssignmentOptimizerAccessor.class
          .getName());

  public static String RESPONSE_HEADER_KEY = "Setting";
  public static String[] RESPONSE_HEADER_FIELDS =
      new String[]{"instanceFilter", "resourceFilter", "returnFormat"};

  private static class InputFields {
    Set<String> activatedInstances = new HashSet<>(); // active = online + enabled.
    Set<String> deactivatedInstances = new HashSet<>(); // deactivate = offline + disabled.
    Set<String> instanceConfigs = new HashSet<>(); // instance configs to be overriden.
    Set<String> instanceFilter = new HashSet<>();
    Set<String> resourceFilter = new HashSet<>();
    AssignmentFormat returnFormat = AssignmentFormat.IdealStateFormat;
  }

  // TODO: We could add a data cache here to avoid read latency.
  private static class ClusterState {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    ClusterConfig clusterConfig;
    List<String> resources = new ArrayList<>();
    List<String> liveInstances; // cluster LiveInstance + activatedInstances - deactivatedInstances
  }

  // Result format. User can choose from IdealState or CurrentState format,
  // IdealState format   : Map of resource -> partition -> instance -> state.  (default)
  // CurrentState format : Map of instance -> resource -> partition -> state.
  private static class AssignmentResult extends HashMap<String, Map<String, Map<String, String>>> {
    public AssignmentResult() {
      super();
    }
  }

  private static class InputJsonContent {
    @JsonProperty("InstanceChange")
    InstanceChangeMap instanceChangeMap;
    @JsonProperty("Options")
    OptionsMap optionsMap;
  }

  private static class InstanceChangeMap {
    @JsonProperty("InstanceConfigs")
    JsonNode instanceConfigs;
    @JsonProperty("ActivateInstances")
    List<String> activateInstances;
    @JsonProperty("DeactivateInstances")
    List<String> deactivateInstances;
  }

  private enum AssignmentFormat {
    IdealStateFormat,
    CurrentStateFormat
  }

  private static class OptionsMap {
    @JsonProperty("InstanceFilter")
    Set<String> instanceFilter;
    @JsonProperty("ResourceFilter")
    Set<String> resourceFilter;
    @JsonProperty("ReturnFormat")
    AssignmentFormat returnFormat;
  }

  @ResponseMetered(name = HttpConstants.WRITE_REQUEST)
  @Timed(name = HttpConstants.WRITE_REQUEST)
  @POST
  public Response computePotentialAssignment(@PathParam("clusterId") String clusterId,
      String content) {

    InputFields inputFields;
    ClusterState clusterState;
    AssignmentResult result;

    try {
      // 1.  Try to parse the content string. If parseable, use it as a KV map. Otherwise, return a REASON String
      inputFields = readInput(content);
      // 2. Read cluster status from ZK.
      clusterState = readClusterStateAndValidateInput(clusterId, inputFields);
      // 3. Call rebalancer tools for each resource.
      result = computeOptimalAssignmentForResources(inputFields, clusterState, clusterId);
      // 4. Serialize result to JSON and return.
      // TODO: We will need to include user input to response header since user may do async call.
      return JSONRepresentation(result, RESPONSE_HEADER_KEY, buildResponseHeaders(inputFields));
    } catch (InvalidParameterException ex) {
      return badRequest(ex.getMessage());
    } catch (JsonProcessingException e) {
      return badRequest("Invalid input: Input can not be parsed into a KV map." + e.getMessage());
    } catch (OutOfMemoryError e) {
      LOG.error("OutOfMemoryError while calling partitionAssignment", e);
      return badRequest(
          "Response size is too large to serialize. Please query by resources or instance filter");
    } catch (Exception e) {
      LOG.error("Failed to compute partition assignment", e);
      return badRequest("Failed to compute partition assignment: " + e);
    }
  }

  private InputFields readInput(String content)
      throws JsonProcessingException, IllegalArgumentException {

    ObjectMapper objectMapper = new ObjectMapper();
    InputJsonContent inputJsonContent = objectMapper.readValue(content, InputJsonContent.class);
    InputFields inputFields = new InputFields();

    if (inputJsonContent.instanceChangeMap != null) {
      Optional.ofNullable(inputJsonContent.instanceChangeMap.activateInstances)
          .ifPresent(inputFields.activatedInstances::addAll);
      Optional.ofNullable(inputJsonContent.instanceChangeMap.deactivateInstances)
          .ifPresent(inputFields.deactivatedInstances::addAll);
      Optional.ofNullable(inputJsonContent.instanceChangeMap.instanceConfigs).ifPresent(
          configs -> configs.forEach(
              instanceConfig -> inputFields.instanceConfigs.add(instanceConfig.toString())));
    }
    if (inputJsonContent.optionsMap != null) {
      Optional.ofNullable(inputJsonContent.optionsMap.resourceFilter)
          .ifPresent(inputFields.resourceFilter::addAll);
      Optional.ofNullable(inputJsonContent.optionsMap.instanceFilter)
          .ifPresent(inputFields.instanceFilter::addAll);
      inputFields.returnFormat = Optional.ofNullable(inputJsonContent.optionsMap.returnFormat)
          .orElse(AssignmentFormat.IdealStateFormat);
    }

    return inputFields;
  }

  private ClusterState readClusterStateAndValidateInput(String clusterId, InputFields inputFields)
      throws InvalidParameterException {

    // One instance can only exist in one of the list in InstanceChange.
    // Validate the intersection is empty.
    validateNoIntxnInstanceChange(inputFields);

    // Add instances to current liveInstances
    ClusterState clusterState = new ClusterState();
    ConfigAccessor cfgAccessor = getConfigAccessor();
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    clusterState.resources = dataAccessor.getChildNames(dataAccessor.keyBuilder().idealStates());
    // Add existing live instances and new instances from user input to instances list.
    Set<String> liveInstancesSet =
        new HashSet<>(dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances()));
    liveInstancesSet.addAll(inputFields.activatedInstances);
    liveInstancesSet.removeAll(inputFields.deactivatedInstances);

    Map<String, InstanceConfig> instanceConfigMap =
        dataAccessor.getChildValuesMap(dataAccessor.keyBuilder().instanceConfigs(), true);

    // Override instance config with inputFields.instanceConfigs
    for (String instanceConfig : inputFields.instanceConfigs) {
      try {
        InstanceConfig instanceConfigOverride = new InstanceConfig(toZNRecord(instanceConfig));
        instanceConfigMap.put(instanceConfigOverride.getInstanceName(), instanceConfigOverride);
      } catch (Exception e) {
        throw new InvalidParameterException(
            "instanceConfig: " + instanceConfig + "is not a valid instanceConfig");
      }
    }

    // Read instance and cluster config.
    // Throw exception if there is no instanceConfig for activatedInstances instance.
    for (String instance : inputFields.activatedInstances) {
      if (instanceConfigMap.containsKey(instance)) {
        instanceConfigMap.get(instance).setInstanceEnabled(true);
      } else {
        throw new InvalidParameterException(
            "instance: " + instance + "does not have instanceConfig");
      }
    }

    for (String instance : inputFields.deactivatedInstances) {
      if (instanceConfigMap.containsKey(instance)) {
        instanceConfigMap.get(instance).setInstanceEnabled(false);
      }
    }

    // We will not consider delayed rebalance. The current implementation of
    // 'getIdealAssignmentForFullAuto' for ChrushEd resources will not consider delayed rebalancer
    // but `getImmediateAssignmentForWagedFullAuto` will honor current timestamp and delayed
    // rebalance window. We are disabling delayed rebalance for now. Could add a cluster option to
    // honor delayed rebalance window in the future.
    ClusterConfig clusterConfig = cfgAccessor.getClusterConfig(clusterId);
    clusterConfig.setDelayRebalaceEnabled(false);
    clusterState.clusterConfig = clusterConfig;
    clusterState.liveInstances = new ArrayList<>(liveInstancesSet);
    clusterState.instanceConfigs.addAll(instanceConfigMap.values());
    return clusterState;
  }

  private AssignmentResult computeOptimalAssignmentForResources(InputFields inputFields,
      ClusterState clusterState, String clusterId) throws Exception {

    AssignmentResult result = new AssignmentResult();
    // Iterate through resources, read resource level info and get potential assignment.
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    List<IdealState> wagedResourceIdealState = new ArrayList<>();

    for (String resource : clusterState.resources) {
      IdealState idealState =
          dataAccessor.getProperty(dataAccessor.keyBuilder().idealStates(resource));
      // Compute all Waged resources in a batch later.
      if (idealState.getRebalancerClassName() != null && idealState.getRebalancerClassName()
          .equals(WagedRebalancer.class.getName())) {
        wagedResourceIdealState.add(idealState);
        continue;
      }
      // For non Waged resources, we don't compute resources not in white list.
      if (!inputFields.resourceFilter.isEmpty() && !inputFields.resourceFilter.contains(resource)) {
        continue;
      }
      // Use getIdealAssignmentForFullAuto for FULL_AUTO resource.
      Map<String, Map<String, String>> partitionAssignments;
      if (idealState.getRebalanceMode() == IdealState.RebalanceMode.FULL_AUTO) {
        String rebalanceStrategy = idealState.getRebalanceStrategy();
        if (rebalanceStrategy == null || rebalanceStrategy
            .equalsIgnoreCase(RebalanceStrategy.DEFAULT_REBALANCE_STRATEGY)) {
          rebalanceStrategy = AutoRebalanceStrategy.class.getName();
        }
        partitionAssignments = new TreeMap<>(HelixUtil
            .getIdealAssignmentForFullAuto(clusterState.clusterConfig, clusterState.instanceConfigs,
                clusterState.liveInstances, idealState, new ArrayList<>(idealState.getPartitionSet()),
                rebalanceStrategy));
        instanceFilter(inputFields, partitionAssignments, resource, result);
      } else if (idealState.getRebalanceMode() == IdealState.RebalanceMode.SEMI_AUTO) {
        LOG.error(
            "Resource" + resource + "is in SEMI_AUTO mode. Skip partition assignment computation.");
      }
    }

    if (!wagedResourceIdealState.isEmpty()) {
      computeWagedAssignmentResult(wagedResourceIdealState, inputFields, clusterState, clusterId,
          result);
    }

    return updateAssignmentFormat(inputFields, result);
  }

  // IdealState format   : Map of resource -> partition -> instance -> state.  (default)
  // CurrentState format : Map of instance -> resource -> partition -> state.
  private AssignmentResult updateAssignmentFormat(InputFields inputFields,
      AssignmentResult idealStateFormatResult) {

    if (inputFields.returnFormat.equals(AssignmentFormat.CurrentStateFormat)) {
      AssignmentResult currentStateFormatResult = new AssignmentResult();
      idealStateFormatResult.forEach((resourceKey, partitionMap) -> partitionMap.forEach(
          (partitionKey, instanceMap) -> instanceMap.forEach(
              (instanceKey, instanceState) -> currentStateFormatResult
                  .computeIfAbsent(instanceKey, x -> new HashMap<>())
                  .computeIfAbsent(resourceKey, y -> new HashMap<>())
                  .put(partitionKey, instanceState))));
      return currentStateFormatResult;
    }
    return idealStateFormatResult;
  }

  /*
  * Return if there are instance exists in more than one lists in InstanceChangeMap.
  */
  private void validateNoIntxnInstanceChange(InputFields inputFields) {
    Set<String> tempSet = new HashSet<>();
    List<Collection<String>> inputs = new ArrayList<>();
    inputs.add(inputFields.activatedInstances);
    inputs.add(inputFields.deactivatedInstances);
    inputs.sort(Comparator.comparingInt(Collection::size));

    for (int i = 0; i < inputs.size() - 1; ++i) {
      for (String s : inputs.get(i)) {
        if (!tempSet.add(s)) {
          throw new InvalidParameterException("Invalid input: instance [" + s
              + "] exist in more than one field in InstanceChange.");
        }
      }
    }
    for (String s : inputs.get(inputs.size() - 1)) {
      if (tempSet.contains(s)) {
        throw new InvalidParameterException(
            "Invalid input: instance [" + s + "] exist in more than one field in InstanceChange.");
      }
    }
  }

  private void computeWagedAssignmentResult(List<IdealState> wagedResourceIdealState,
      InputFields inputFields, ClusterState clusterState, String clusterId,
      AssignmentResult result) {

    // If the cluster is in Maintenance mode, throw an exception
    // TODO: we should return the partitionAssignment regardless of the cluster is in Maintenance
    // mode or not
    // if (getHelixAdmin().isInMaintenanceMode(clusterId)) {
    //   throw new UnsupportedOperationException(
    //       "Can not query potential Assignment when cluster is in Maintenance mode.");
    // }

    // Use getTargetAssignmentForWagedFullAuto for Waged resources.
    ConfigAccessor cfgAccessor = getConfigAccessor();
    List<ResourceConfig> wagedResourceConfigs = new ArrayList<>();
    for (IdealState idealState : wagedResourceIdealState) {
      ResourceConfig resourceConfig = cfgAccessor.getResourceConfig(clusterId, idealState.getResourceName());
      if (resourceConfig != null) {
        wagedResourceConfigs.add(resourceConfig);
      }
    }

    Map<String, ResourceAssignment> wagedAssignmentResult;
    wagedAssignmentResult = HelixUtil.getTargetAssignmentForWagedFullAuto(getZkBucketDataAccessor(),
        new ZkBaseDataAccessor<>(getRealmAwareZkClient()), clusterState.clusterConfig,
        clusterState.instanceConfigs, clusterState.liveInstances, wagedResourceIdealState,
        wagedResourceConfigs);

    // Convert ResourceAssignment to plain map.
    for (Map.Entry<String, ResourceAssignment> wagedAssignment : wagedAssignmentResult.entrySet()) {
      String resource = wagedAssignment.getKey();
      if (!inputFields.resourceFilter.isEmpty() && !inputFields.resourceFilter.contains(resource)) {
        continue;
      }
      Map<String, Map<String, String>> partitionAssignments = new TreeMap<>();
      wagedAssignment.getValue().getMappedPartitions().forEach(partition -> partitionAssignments
          .put(partition.getPartitionName(), wagedAssignment.getValue().getReplicaMap(partition)));
      instanceFilter(inputFields, partitionAssignments, resource, result);
    }
  }

  private void instanceFilter(InputFields inputFields,
      Map<String, Map<String, String>> partitionAssignments, String resource,
      AssignmentResult result) {

    if (!inputFields.instanceFilter.isEmpty()) {
      for (Iterator<Map.Entry<String, Map<String, String>>> partitionAssignmentIt =
          partitionAssignments.entrySet().iterator(); partitionAssignmentIt.hasNext(); ) {
        Map.Entry<String, Map<String, String>> partitionAssignment = partitionAssignmentIt.next();
        Map<String, String> instanceStates = partitionAssignment.getValue();
        // Only keep instance in instanceFilter
        instanceStates.entrySet().removeIf(e ->
            (!inputFields.instanceFilter.isEmpty() && !inputFields.instanceFilter
                .contains(e.getKey())));
        if (instanceStates.isEmpty()) {
          partitionAssignmentIt.remove();
        }
      }
    }
    result.put(resource, partitionAssignments);
  }

  private Map<String, Object> buildResponseHeaders(InputFields inputFields) {
    Map<String, Object> headers = new HashMap<>();
    headers.put(RESPONSE_HEADER_FIELDS[0], inputFields.instanceFilter);
    headers.put(RESPONSE_HEADER_FIELDS[1], inputFields.resourceFilter);
    headers.put(RESPONSE_HEADER_FIELDS[2], inputFields.returnFormat.name());
    return headers;
  }
}
