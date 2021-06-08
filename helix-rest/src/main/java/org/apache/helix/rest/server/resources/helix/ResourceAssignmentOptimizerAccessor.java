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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import com.codahale.metrics.annotation.ResponseMetered;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
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
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.rest.common.HttpConstants;
import org.apache.helix.util.HelixUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@Path("/clusters/{clusterId}/partitionAssignment")
public class ResourceAssignmentOptimizerAccessor extends AbstractHelixResource {
  private static Logger LOG = LoggerFactory.getLogger(
      org.apache.helix.rest.server.resources.helix.ResourceAssignmentOptimizerAccessor.class
          .getName());

  private static class InputFields {
    List<String> newInstances = new ArrayList<>();
    List<String> instancesToRemove = new ArrayList<>();
    Map<String, String> nodeSwap = new HashMap<>(); // old instance -> new instance.
    Set<String> instanceFilter = new HashSet<>();
    Set<String> resourceFilter = new HashSet<>();
    String returnFormat = IDEAL_STATE_FORMAT;
  }

  // TODO: We could add a data cache here to avoid read latency.
  private static class ClusterState {
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    ClusterConfig clusterConfig;
    List<String> resources = new ArrayList<>();
    List<String> instances;        // cluster LiveInstance + addInstances - instancesToRemove.
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
    @JsonProperty("AddInstances")
    List<String> addInstances;
    @JsonProperty("RemoveInstances")
    List<String> removeInstances;
    @JsonProperty("SwapInstances")
    Map<String, String> swapInstances;
  }

  private static final String IDEAL_STATE_FORMAT = "IdealStateFormat";
  private static final String CURRENT_STATE_FORMAT = "CurrentStateFormat";

  private static class OptionsMap {
    @JsonProperty("InstanceFilter")
    Set<String> instanceFilter;
    @JsonProperty("ResourceFilter")
    Set<String> resourceFilter;
    @JsonProperty("ReturnFormat")
    String returnFormat;
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
      return JSONRepresentation(result);
    } catch (InvalidParameterException ex) {
      return badRequest(ex.getMessage());
    } catch (JsonProcessingException e) {
      return badRequest("Invalid input: Input can not be parsed into a KV map." + e.getMessage());
    } catch (OutOfMemoryError e) {
      LOG.error("OutOfMemoryError while calling partitionAssignment" + Arrays
          .toString(e.getStackTrace()));
      return badRequest(
          "Response size is too large to serialize. Please query by resources or instance filter");
    } catch (Exception e) {
      LOG.error("Failed to compute partition assignment:" + Arrays.toString(e.getStackTrace()));
      return badRequest("Failed to compute partition assignment: " + e);
    }
  }

  private InputFields readInput(String content)
      throws InvalidParameterException, JsonProcessingException {

    ObjectMapper objectMapper = new ObjectMapper();
    InputJsonContent inputJsonContent = objectMapper.readValue(content, InputJsonContent.class);
    InputFields inputFields = new InputFields();

    if (inputJsonContent.instanceChangeMap != null) {
      Optional.ofNullable(inputJsonContent.instanceChangeMap.addInstances)
          .ifPresent(inputFields.newInstances::addAll);
      Optional.ofNullable(inputJsonContent.instanceChangeMap.removeInstances)
          .ifPresent(inputFields.instancesToRemove::addAll);
      Optional.ofNullable(inputJsonContent.instanceChangeMap.swapInstances)
          .ifPresent(inputFields.nodeSwap::putAll);
    }
    if (inputJsonContent.optionsMap != null) {
      Optional.ofNullable(inputJsonContent.optionsMap.resourceFilter)
          .ifPresent(inputFields.resourceFilter::addAll);
      Optional.ofNullable(inputJsonContent.optionsMap.instanceFilter)
          .ifPresent(inputFields.instanceFilter::addAll);
      inputFields.returnFormat =
          Optional.ofNullable(inputJsonContent.optionsMap.returnFormat).orElse(IDEAL_STATE_FORMAT);
    }

    return inputFields;
  }

  private ClusterState readClusterStateAndValidateInput(String clusterId, InputFields inputFields)
      throws InvalidParameterException {

    ClusterState clusterState = new ClusterState();
    ConfigAccessor cfgAccessor = getConfigAccessor();
    HelixDataAccessor dataAccessor = getDataAccssor(clusterId);
    clusterState.resources = dataAccessor.getChildNames(dataAccessor.keyBuilder().idealStates());
    // Add existing live instances and new instances from user input to instances list.
    clusterState.instances = dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances());
    clusterState.instances.addAll(inputFields.newInstances);

    // Check if to be removed instances and old instances in swap node exist in live instance.
    if (!inputFields.nodeSwap.isEmpty() || !inputFields.instancesToRemove.isEmpty()) {
      Set<String> liveInstanceSet = new HashSet<>(clusterState.instances);
      for (Map.Entry<String, String> nodeSwapPair : inputFields.nodeSwap.entrySet()) {
        if (!liveInstanceSet.contains(nodeSwapPair.getKey())) {
          throw new InvalidParameterException("Invalid input: instance [" + nodeSwapPair.getKey()
              + "] in SwapInstances does not exist in cluster.");
        }
      }
      for (String instanceToRemove : inputFields.instancesToRemove) {
        if (!liveInstanceSet.contains(instanceToRemove)) {
          throw new InvalidParameterException("Invalid input: instance [" + instanceToRemove
              + "] in RemoveInstances does not exist in cluster.");
        }
      }
      if (!inputFields.instancesToRemove.isEmpty()) {
        clusterState.instances.removeIf(inputFields.instancesToRemove::contains);
      }
    }

    // Read instance and cluster config.
    // It will throw exception if there is no instanceConfig for newly added instance.
    for (String instance : clusterState.instances) {
      InstanceConfig config = cfgAccessor.getInstanceConfig(clusterId, instance);
      clusterState.instanceConfigs.add(config);
    }
    clusterState.clusterConfig = cfgAccessor.getClusterConfig(clusterId);

    if (!inputFields.returnFormat.equals(CURRENT_STATE_FORMAT) &&
        !inputFields.returnFormat.equals(IDEAL_STATE_FORMAT)) {
      throw new InvalidParameterException(
          "Invalid input: Options.ReturnFormat [" + inputFields.returnFormat + "] is invalid.");
    }

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
                clusterState.instances, idealState, new ArrayList<>(idealState.getPartitionSet()),
                rebalanceStrategy));
        instanceSwapAndFilter(inputFields, partitionAssignments, resource, result);
      } else if (idealState.getRebalanceMode() == IdealState.RebalanceMode.SEMI_AUTO) {
        // Use computeIdealMapping for SEMI_AUTO resource.
        Map<String, List<String>> preferenceLists = idealState.getPreferenceLists();
        partitionAssignments = new TreeMap<>();
        HashSet<String> liveInstances = new HashSet<>(clusterState.instances);
        List<String> disabledInstance =
            clusterState.instanceConfigs.stream().filter(enabled -> !enabled.getInstanceEnabled())
                .map(InstanceConfig::getInstanceName).collect(Collectors.toList());
        liveInstances.removeAll(disabledInstance);
        StateModelDefinition stateModelDef = dataAccessor
            .getProperty(dataAccessor.keyBuilder().stateModelDef(idealState.getStateModelDefRef()));
        for (String partitionName : preferenceLists.keySet()) {
          if (!preferenceLists.get(partitionName).isEmpty() && preferenceLists.get(partitionName)
              .get(0)
              .equalsIgnoreCase(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name())) {
            partitionAssignments.put(partitionName, HelixUtil
                .computeIdealMapping(clusterState.instances, stateModelDef, liveInstances));
          } else {
            partitionAssignments.put(partitionName, HelixUtil
                .computeIdealMapping(preferenceLists.get(partitionName), stateModelDef,
                    liveInstances));
          }
        }
        instanceSwapAndFilter(inputFields, partitionAssignments, resource, result);
      }
    }

    if (!wagedResourceIdealState.isEmpty()) {
      computeWagedAssignmentResult(wagedResourceIdealState, inputFields, clusterState, clusterId,
          result);
    }

    if (inputFields.returnFormat.equals(CURRENT_STATE_FORMAT)) {
      return changeToCurrentStateFormat(result);
    } else {
      return result;
    }
  }

  // IdealState format   : Map of resource -> partition -> instance -> state.  (default)
  // CurrentState format : Map of instance -> resource -> partition -> state.
  private AssignmentResult changeToCurrentStateFormat(AssignmentResult idealStateFormatResult) {
    AssignmentResult currentStateFormatResult = new AssignmentResult();

    idealStateFormatResult.forEach((resourceKey, partitionMap) -> partitionMap.forEach(
        (partitionKey, instanceMap) -> instanceMap.forEach(
            (instanceKey, instanceState) -> currentStateFormatResult
                .computeIfAbsent(instanceKey, x -> new HashMap<>())
                .computeIfAbsent(resourceKey, y -> new HashMap<>())
                .put(partitionKey, instanceState))));

    return currentStateFormatResult;
  }

  private void computeWagedAssignmentResult(List<IdealState> wagedResourceIdealState,
      InputFields inputFields, ClusterState clusterState, String clusterId,
      AssignmentResult result) {

    // Use getTargetAssignmentForWagedFullAuto for Waged resources.
    ConfigAccessor cfgAccessor = getConfigAccessor();
    List<ResourceConfig> wagedResourceConfigs = new ArrayList<>();
    for (IdealState idealState : wagedResourceIdealState) {
      wagedResourceConfigs
          .add(cfgAccessor.getResourceConfig(clusterId, idealState.getResourceName()));
    }

    Map<String, ResourceAssignment> wagedAssignmentResult;
    wagedAssignmentResult = HelixUtil.getTargetAssignmentForWagedFullAuto(getZkBucketDataAccessor(),
        new ZkBaseDataAccessor<>(getRealmAwareZkClient()), clusterState.clusterConfig,
        clusterState.instanceConfigs, clusterState.instances, wagedResourceIdealState,
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
      instanceSwapAndFilter(inputFields, partitionAssignments, resource, result);
    }
  }

  private void instanceSwapAndFilter(InputFields inputFields,
      Map<String, Map<String, String>> partitionAssignments, String resource,
      AssignmentResult result) {

    if (!inputFields.nodeSwap.isEmpty() || !inputFields.instanceFilter.isEmpty()) {
      for (Iterator<Map.Entry<String, Map<String, String>>> partitionAssignmentIt =
          partitionAssignments.entrySet().iterator(); partitionAssignmentIt.hasNext(); ) {
        Map.Entry<String, Map<String, String>> partitionAssignment = partitionAssignmentIt.next();
        Map<String, String> instanceStates = partitionAssignment.getValue();
        Map<String, String> tempInstanceState = new HashMap<>();
        // Add new pairs to tempInstanceState
        instanceStates.entrySet().stream()
            .filter(entry -> inputFields.nodeSwap.containsKey(entry.getKey())).forEach(
            entry -> tempInstanceState
                .put(inputFields.nodeSwap.get(entry.getKey()), entry.getValue()));
        instanceStates.putAll(tempInstanceState);
        // Only keep instance in instanceFilter
        instanceStates.entrySet().removeIf(e ->
            (!inputFields.instanceFilter.isEmpty() && !inputFields.instanceFilter
                .contains(e.getKey())) || inputFields.nodeSwap.containsKey(e.getKey()));
        if (instanceStates.isEmpty()) {
          partitionAssignmentIt.remove();
        }
      }
    }
    result.put(resource, partitionAssignments);
  }
}