package org.apache.helix.rest.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.rest.clusterMaintenanceService.MaintenanceManagementService;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.rest.server.json.instance.StoppableCheck;

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
public class StoppableInstancesSelector {
  // This type does not belong to real HealthCheck failed reason. Also, if we add this type
  // to HealthCheck enum, it could introduce more unnecessary check step since the InstanceServiceImpl
  // loops all the types to do corresponding checks.
  private final static String INSTANCE_NOT_EXIST = "HELIX:INSTANCE_NOT_EXIST";
  private String _clusterId;
  private List<String> _orderOfZone;
  private String _customizedInput;
  private ArrayNode _stoppableInstances;
  private ObjectNode _failedStoppableInstances;
  private MaintenanceManagementService _maintenanceService;
  private ClusterTopology _clusterTopology;

  public StoppableInstancesSelector(String clusterId, List<String> orderOfZone,
      String customizedInput, ArrayNode stoppableInstances, ObjectNode failedStoppableInstances,
      MaintenanceManagementService maintenanceService, ClusterTopology clusterTopology) {
    _clusterId = clusterId;
    _orderOfZone = orderOfZone;
    _customizedInput = customizedInput;
    _stoppableInstances = stoppableInstances;
    _failedStoppableInstances = failedStoppableInstances;
    _maintenanceService = maintenanceService;
    _clusterTopology = clusterTopology;
  }

  /**
   * Evaluates and collects stoppable instances within a specified or determined zone based on the order of zones.
   * If _orderOfZone is specified, the method targets the first non-empty zone; otherwise, it targets the zone with
   * the highest instance count. The method iterates through instances, performing stoppable checks, and records
   * reasons for non-stoppability.
   *
   * @param instances A list of instance to be evaluated.
   * @throws IOException
   */
  public void getStoppableInstancesInSingleZone(List<String> instances) throws IOException {
    List<String> zoneBasedInstance =
        getZoneBasedInstances(instances, _orderOfZone, _clusterTopology.toZoneMapping());
    Map<String, StoppableCheck> instancesStoppableChecks =
        _maintenanceService.batchGetInstancesStoppableChecks(_clusterId, zoneBasedInstance,
            _customizedInput);
    for (Map.Entry<String, StoppableCheck> instanceStoppableCheck : instancesStoppableChecks.entrySet()) {
      String instance = instanceStoppableCheck.getKey();
      StoppableCheck stoppableCheck = instanceStoppableCheck.getValue();
      if (!stoppableCheck.isStoppable()) {
        ArrayNode failedReasonsNode = _failedStoppableInstances.putArray(instance);
        for (String failedReason : stoppableCheck.getFailedChecks()) {
          failedReasonsNode.add(JsonNodeFactory.instance.textNode(failedReason));
        }
      } else {
        _stoppableInstances.add(instance);
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
    nonSelectedInstances.removeAll(_clusterTopology.getAllInstances());
    for (String nonSelectedInstance : nonSelectedInstances) {
      ArrayNode failedReasonsNode = _failedStoppableInstances.putArray(nonSelectedInstance);
      failedReasonsNode.add(JsonNodeFactory.instance.textNode(INSTANCE_NOT_EXIST));
    }
  }

  public void getStoppableInstancesCrossZones() {
    // TODO: Add implementation to enable cross zone stoppable check.
    throw new NotImplementedException("Not Implemented");
  }

  /**
   * Get instances belongs to the first zone. If the zone is already empty, Helix will iterate zones
   * by order until find the zone contains instances.
   *
   * The order of zones can directly come from user input. If user did not specify it, Helix will order
   * zones by the number of associated instances in descending order.
   *
   * @param instances
   * @param orderedZones
   * @return
   */
  private List<String> getZoneBasedInstances(List<String> instances, List<String> orderedZones,
      Map<String, Set<String>> zoneMapping) {

    // If the orderedZones is not specified, we will order all zones by their instances count in descending order.
    if (orderedZones == null) {
      orderedZones = new ArrayList<>(getOrderedZoneToInstancesMap(zoneMapping).keySet());
    }

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

  public static class StoppableInstancesSelectorBuilder {
    private String _clusterId;
    private List<String> _orderOfZone;
    private String _customizedInput;
    private ArrayNode _stoppableInstances;
    private ObjectNode _failedStoppableInstances;
    private MaintenanceManagementService _maintenanceService;
    private ClusterTopology _clusterTopology;

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

    public StoppableInstancesSelectorBuilder setStoppableInstances(ArrayNode stoppableInstances) {
      _stoppableInstances = stoppableInstances;
      return this;
    }

    public StoppableInstancesSelectorBuilder setFailedStoppableInstances(ObjectNode failedStoppableInstances) {
      _failedStoppableInstances = failedStoppableInstances;
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

    public StoppableInstancesSelector build() {
      return new StoppableInstancesSelector(_clusterId, _orderOfZone,
          _customizedInput, _stoppableInstances, _failedStoppableInstances, _maintenanceService,
          _clusterTopology);
    }
  }
}
