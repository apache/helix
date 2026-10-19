package org.apache.helix.rest.clusterMaintenanceService;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.helix.PropertyKey;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.json.instance.StoppableCheck;
import org.apache.helix.rest.server.resources.helix.InstancesAccessor;

/**
 * This class is used to select stoppable instances based on different selection criteria.
 * Selection criteria include:
 * 1. Zone-based selection - Select instances from a single zone
 * 2. Cross-zone selection - Select instances across multiple zones
 * 3. Non-zone-based selection - Select instances regardless of zone
 *
 * For zone-based selection, instances can be ordered either lexicographically (default) or
 * by preserving the original input order when preserveOrder is set to true.
 */
public class StoppableInstancesSelector {
  // This type does not belong to real HealthCheck failed reason. Also, if we add this type
  // to HealthCheck enum, it could introduce more unnecessary check step since the InstanceServiceImpl
  // loops all the types to do corresponding checks.
  private final static String INSTANCE_NOT_EXIST = "HELIX:INSTANCE_NOT_EXIST";
  private final String _clusterId;
  private List<String> _orderOfZone;
  private final String _customizedInput;
  private final MaintenanceManagementService _maintenanceService;
  private final ClusterTopology _clusterTopology;
  private final ZKHelixDataAccessor _dataAccessor;

  private StoppableInstancesSelector(String clusterId, List<String> orderOfZone,
      String customizedInput, MaintenanceManagementService maintenanceService,
      ClusterTopology clusterTopology, ZKHelixDataAccessor dataAccessor) {
    _clusterId = clusterId;
    _orderOfZone = orderOfZone;
    _customizedInput = customizedInput;
    _maintenanceService = maintenanceService;
    _clusterTopology = clusterTopology;
    _dataAccessor = dataAccessor;
  }

  /**
   * Evaluates and collects stoppable instances within a specified or determined zone based on the order of zones.
   * If _orderOfZone is specified, the method targets the first non-empty zone; otherwise, it targets the zone with
   * the highest instance count. The method iterates through instances, performing stoppable checks, and records
   * reasons for non-stoppability.
   *
   * @param instances A list of instance to be evaluated.
   * @param toBeStoppedInstances A list of instances presumed to be already stopped
   * @return An ObjectNode containing:
   *         - 'stoppableNode': List of instances that can be stopped.
   *         - 'instance_not_stoppable_with_reasons': A map with the instance name as the key and
   *         a list of reasons for non-stoppability as the value.
   * @throws IOException
   */
  public ObjectNode getStoppableInstancesInSingleZone(List<String> instances,
      List<String> toBeStoppedInstances) throws IOException {
    return getStoppableInstancesInSingleZone(instances, toBeStoppedInstances, false);
  }

  /**
   * Evaluates and collects stoppable instances within a specified or determined zone based on the order of zones.
   * If _orderOfZone is specified, the method targets the first non-empty zone; otherwise, it targets the zone with
   * the highest instance count. The method iterates through instances, performing stoppable checks, and records
   * reasons for non-stoppability.
   *
   * @param instances A list of instance to be evaluated.
   * @param toBeStoppedInstances A list of instances presumed to be already stopped
   * @param preserveOrder Indicates whether to preserve the original order of instances
   * @return An ObjectNode containing:
   *         - 'stoppableNode': List of instances that can be stopped.
   *         - 'instance_not_stoppable_with_reasons': A map with the instance name as the key and
   *         a list of reasons for non-stoppability as the value.
   * @throws IOException
   */
  public ObjectNode getStoppableInstancesInSingleZone(List<String> instances,
      List<String> toBeStoppedInstances, boolean preserveOrder) throws IOException {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    ArrayNode stoppableInstances =
        result.putArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    ObjectNode failedStoppableInstances = result.putObject(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Set<String> toBeStoppedInstancesSet = findToBeStoppedInstances(toBeStoppedInstances);

    List<String> zoneBasedInstance =
        getZoneBasedInstances(instances, _clusterTopology.toZoneMapping(), preserveOrder);
    populateStoppableInstances(zoneBasedInstance, toBeStoppedInstancesSet, stoppableInstances,
        failedStoppableInstances, preserveOrder);
    processNonexistentInstances(instances, failedStoppableInstances);

    return result;
  }

  /**
   * Evaluates and collects stoppable instances cross a set of zones based on the order of zones.
   * The method iterates through instances, performing stoppable checks, and records reasons for
   * non-stoppability.
   *
   * @param instances A list of instance to be evaluated.
   * @param toBeStoppedInstances A list of instances presumed to be already stopped
   * @return An ObjectNode containing:
   *         - 'stoppableNode': List of instances that can be stopped.
   *         - 'instance_not_stoppable_with_reasons': A map with the instance name as the key and
   *         a list of reasons for non-stoppability as the value.
   * @throws IOException
   */
  public ObjectNode getStoppableInstancesCrossZones(List<String> instances,
      List<String> toBeStoppedInstances) throws IOException {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    ArrayNode stoppableInstances =
        result.putArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    ObjectNode failedStoppableInstances = result.putObject(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Set<String> toBeStoppedInstancesSet = findToBeStoppedInstances(toBeStoppedInstances);

    Map<String, Set<String>> zoneMapping = _clusterTopology.toZoneMapping();
    for (String zone : _orderOfZone) {
      Set<String> instanceSet = new HashSet<>(instances);
      Set<String> currentZoneInstanceSet = new HashSet<>(zoneMapping.get(zone));
      instanceSet.retainAll(currentZoneInstanceSet);
      if (instanceSet.isEmpty()) {
        continue;
      }
      populateStoppableInstances(new ArrayList<>(instanceSet), toBeStoppedInstancesSet, stoppableInstances,
          failedStoppableInstances, false);
    }
    processNonexistentInstances(instances, failedStoppableInstances);
    return result;
  }

  /**
   * Evaluates and collects stoppable instances not based on the zone order.
   * The method iterates through instances, performing stoppable checks, and records reasons for
   * non-stoppability.
   *
   * @param instances A list of instance to be evaluated.
   * @param toBeStoppedInstances A list of instances presumed to be already stopped
   * @return An ObjectNode containing:
   *         - 'stoppableNode': List of instances that can be stopped.
   *         - 'instance_not_stoppable_with_reasons': A map with the instance name as the key and
   *         a list of reasons for non-stoppability as the value.
   * @throws IOException
   */
  public ObjectNode getStoppableInstancesNonZoneBased(List<String> instances,
      List<String> toBeStoppedInstances) throws IOException {
    ObjectNode result = JsonNodeFactory.instance.objectNode();
    ArrayNode stoppableInstances =
        result.putArray(InstancesAccessor.InstancesProperties.instance_stoppable_parallel.name());
    ObjectNode failedStoppableInstances = result.putObject(
        InstancesAccessor.InstancesProperties.instance_not_stoppable_with_reasons.name());
    Set<String> toBeStoppedInstancesSet = findToBeStoppedInstances(toBeStoppedInstances);

    // Because zone order calculation is omitted, we must verify each instance's existence
    // to ensure we only process valid instances before performing stoppable check.
    Set<String> nonExistingInstances = processNonexistentInstances(instances, failedStoppableInstances);
    List<String> instancesToCheck = new ArrayList<>(instances);
    instancesToCheck.removeAll(nonExistingInstances);
    populateStoppableInstances(instancesToCheck, toBeStoppedInstancesSet, stoppableInstances,
        failedStoppableInstances, false);

    return result;
  }

  private void populateStoppableInstances(List<String> instances, Set<String> toBeStoppedInstances,
      ArrayNode stoppableInstances, ObjectNode failedStoppableInstances, boolean preserveOrder) throws IOException {
    Map<String, StoppableCheck> instancesStoppableChecks =
        _maintenanceService.batchGetInstancesStoppableChecks(_clusterId, instances,
            _customizedInput, toBeStoppedInstances, preserveOrder);

    for (Map.Entry<String, StoppableCheck> instanceStoppableCheck : instancesStoppableChecks.entrySet()) {
      String instance = instanceStoppableCheck.getKey();
      StoppableCheck stoppableCheck = instanceStoppableCheck.getValue();
      if (!stoppableCheck.isStoppable()) {
        ArrayNode failedReasonsNode = failedStoppableInstances.putArray(instance);
        for (String failedReason : stoppableCheck.getFailedChecks()) {
          failedReasonsNode.add(JsonNodeFactory.instance.textNode(failedReason));
        }
      } else {
        stoppableInstances.add(instance);
        // Update the toBeStoppedInstances set with the currently identified stoppable instance.
        // This ensures that subsequent checks in other zones are aware of this instance's stoppable status.
        toBeStoppedInstances.add(instance);
      }
    }
  }

  private Set<String> processNonexistentInstances(List<String> instances, ObjectNode failedStoppableInstances) {
    // Adding following logic to check whether instances exist or not. An instance exist could be
    // checking following scenario:
    // 1. Instance got dropped. (InstanceConfig is gone.)
    // 2. Instance name has typo.

    // If we dont add this check, the instance, which does not exist, will be disappeared from
    // result since Helix skips instances for instances not in the selected zone. User may get
    // confused with the output.
    Set<String> nonSelectedInstances = new HashSet<>(instances);
    nonSelectedInstances.removeAll(_clusterTopology.getAllInstances());
    for (String nonSelectedInstance : nonSelectedInstances) {
      ArrayNode failedReasonsNode = failedStoppableInstances.putArray(nonSelectedInstance);
      failedReasonsNode.add(JsonNodeFactory.instance.textNode(INSTANCE_NOT_EXIST));
    }
    return nonSelectedInstances;
  }

  /**
   * Determines the order of zones. If an order is provided by the user, it will be used directly.
   * Otherwise, zones will be ordered by their associated instance count in descending order.
   *
   * If `random` is true, the order of zones will be randomized regardless of any previous order.
   *
   * @param instances A list of instance to be used to calculate the order of zones.
   * @param random Indicates whether to randomize the order of zones.
   */
  public void calculateOrderOfZone(List<String> instances, boolean random) {
    if (_orderOfZone == null) {
      Map<String, Set<String>> zoneMapping = _clusterTopology.toZoneMapping();
      Map<String, Set<String>> zoneToInstancesMap = new HashMap<>();
      for (ClusterTopology.Zone zone : _clusterTopology.getZones()) {
        Set<String> instanceSet = new HashSet<>(instances);
        // TODO: Use instance config from Helix-rest Cache to get the zone instead of reading the topology info
        Set<String> currentZoneInstanceSet = new HashSet<>(zoneMapping.get(zone.getId()));
        instanceSet.retainAll(currentZoneInstanceSet);
        if (instanceSet.isEmpty()) {
          continue;
        }
        zoneToInstancesMap.put(zone.getId(), instanceSet);
      }

      _orderOfZone = new ArrayList<>(getOrderedZoneToInstancesMap(zoneToInstancesMap).keySet());
    }

    if (_orderOfZone.isEmpty()) {
      return;
    }

    if (random) {
      Collections.shuffle(_orderOfZone);
    }
  }

  /**
   * Get instances belongs to the first zone. If the zone is already empty, Helix will iterate zones
   * by order until find the zone contains instances.
   *
   * The order of zones can directly come from user input. If user did not specify it, Helix will order
   * zones by the number of associated instances in descending order.
   *
   * @param instances List of instances to be considered
   * @param zoneMapping Mapping from zone to instances
   * @param preserveOrder Indicates whether to preserve the original order of instances
   * @return List of instances in the first non-empty zone. If preserveOrder is true, the original order
   *         of instances is maintained. If preserveOrder is false (default), instances are sorted lexicographically.
   */
  private List<String> getZoneBasedInstances(List<String> instances,
      Map<String, Set<String>> zoneMapping, boolean preserveOrder) {
    if (_orderOfZone.isEmpty()) {
      return Collections.emptyList();
    }

    for (String zone : _orderOfZone) {
      Set<String> currentZoneInstanceSet = zoneMapping.get(zone);
      if (currentZoneInstanceSet == null || currentZoneInstanceSet.isEmpty()) {
        continue;
      }

      // Filter instances based on current zone
      List<String> filteredInstances = instances.stream()
          .filter(currentZoneInstanceSet::contains)
          .collect(Collectors.toList());

      if (!filteredInstances.isEmpty()) {
        // If preserve order is not required, return sorted list
        if (!preserveOrder) {
          Collections.sort(filteredInstances); // Lexicographical order
        }
        return filteredInstances;
      }
    }

    return Collections.emptyList();
  }

  /**
   * Returns a map from zone to instances set, ordered by the number of instances in each zone
   * in descending order.
   *
   * @param zoneMapping A map from zone to instances set
   * @return An ordered map from zone to instances set, with zones having more instances appearing first.
   */
  private Map<String, Set<String>> getOrderedZoneToInstancesMap(
      Map<String, Set<String>> zoneMapping) {
    return zoneMapping.entrySet().stream()
        .sorted((e1, e2) -> e2.getValue().size() - e1.getValue().size()).collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (existing, replacement) -> existing, LinkedHashMap::new));
  }

  /**
   * Collect instances within the cluster where the instance operation is set to EVACUATE, SWAP_IN, or UNKNOWN.
   * And return them as a set.
   *
   * @param toBeStoppedInstances A list of instances we presume to be stopped.
   */
  private Set<String> findToBeStoppedInstances(List<String> toBeStoppedInstances) {
    Set<String> toBeStoppedInstancesSet = new HashSet<>(toBeStoppedInstances);
    Set<String> allInstances = _clusterTopology.getAllInstances();
    for (String instance : allInstances) {
      PropertyKey.Builder propertyKeyBuilder = _dataAccessor.keyBuilder();
      InstanceConfig instanceConfig =
          _dataAccessor.getProperty(propertyKeyBuilder.instanceConfig(instance));
      InstanceConstants.InstanceOperation operation = instanceConfig.getInstanceOperation().getOperation();
      if (operation == InstanceConstants.InstanceOperation.EVACUATE
          || operation == InstanceConstants.InstanceOperation.SWAP_IN
          || operation == InstanceConstants.InstanceOperation.UNKNOWN) {
        toBeStoppedInstancesSet.add(instance);
      }
    }
    return toBeStoppedInstancesSet;
  }

  public static class StoppableInstancesSelectorBuilder {
    private String _clusterId;
    private List<String> _orderOfZone;
    private String _customizedInput;
    private MaintenanceManagementService _maintenanceService;
    private ClusterTopology _clusterTopology;
    private ZKHelixDataAccessor _dataAccessor;

    public StoppableInstancesSelectorBuilder setClusterId(String clusterId) {
      _clusterId = clusterId;
      return this;
    }

    public StoppableInstancesSelectorBuilder setOrderOfZone(List<String> orderOfZone) {
      _orderOfZone = orderOfZone;
      return this;
    }

    public StoppableInstancesSelectorBuilder setCustomizedInput(String customizedInput) {
      _customizedInput = customizedInput;
      return this;
    }

    public StoppableInstancesSelectorBuilder setMaintenanceService(
        MaintenanceManagementService maintenanceService) {
      _maintenanceService = maintenanceService;
      return this;
    }

    public StoppableInstancesSelectorBuilder setClusterTopology(ClusterTopology clusterTopology) {
      _clusterTopology = clusterTopology;
      return this;
    }

    public StoppableInstancesSelectorBuilder setDataAccessor(ZKHelixDataAccessor dataAccessor) {
      _dataAccessor = dataAccessor;
      return this;
    }

    public StoppableInstancesSelector build() {
      return new StoppableInstancesSelector(_clusterId, _orderOfZone, _customizedInput,
          _maintenanceService, _clusterTopology, _dataAccessor);
    }
  }
}
