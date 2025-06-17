package org.apache.helix.rest.server.service;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.helix.AccessOption;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.cloud.constants.VirtualTopologyGroupConstants;
import org.apache.helix.cloud.topology.FaultZoneBasedVirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.FifoVirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.InstanceCountImbalanceAlgorithm;
import org.apache.helix.cloud.topology.InstanceWeightImbalanceAlgorithm;
import org.apache.helix.cloud.topology.VirtualGroupAssignmentAlgorithm;
import org.apache.helix.cloud.topology.VirtualGroupImbalanceDetectionAlgorithm;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterTopologyConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.DataUpdater;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.helix.cloud.constants.VirtualTopologyGroupConstants.*;
import static org.apache.helix.util.VirtualTopologyUtil.computeVirtualFaultZoneTypeKey;

/**
 * Service for virtual topology group.
 * It's a virtualization layer on top of physical fault domain and topology in cloud environments.
 * The service computes the mapping from virtual group to instances based on the current cluster topology and update the
 * information to cluster and all instances in the cluster.
 */
public class VirtualTopologyGroupService {
  private static final Logger LOG = LoggerFactory.getLogger(VirtualTopologyGroupService.class);

  private final HelixAdmin _helixAdmin;
  private final ClusterService _clusterService;
  private final ConfigAccessor _configAccessor;
  private final HelixDataAccessor _dataAccessor;
  private VirtualGroupAssignmentAlgorithm _assignmentAlgorithm;
  private VirtualGroupImbalanceDetectionAlgorithm _imbalanceDetectionAlgorithm;

  public VirtualTopologyGroupService(HelixAdmin helixAdmin, ClusterService clusterService,
      ConfigAccessor configAccessor, HelixDataAccessor dataAccessor) {
    _helixAdmin = helixAdmin;
    _clusterService = clusterService;
    _configAccessor = configAccessor;
    _dataAccessor = dataAccessor;
    _assignmentAlgorithm = FifoVirtualGroupAssignmentAlgorithm.getInstance(); // default assignment algorithm
    _imbalanceDetectionAlgorithm = InstanceCountImbalanceAlgorithm.getInstance(); // default imbalance detection algorithm
  }

  /**
   * Add virtual topology group for a cluster.
   * This includes calculating the virtual group assignment for all instances in the cluster then update instance config
   * and cluster config. We override {@link ClusterConfig.ClusterConfigProperty#TOPOLOGY} and
   * {@link ClusterConfig.ClusterConfigProperty#FAULT_ZONE_TYPE} for cluster config, and add new field to
   * {@link InstanceConfig.InstanceConfigProperty#DOMAIN} that contains virtual topology group information.
   * This is only supported for cloud environments. Cluster is expected to be in maintenance mode during config change.
   * @param clusterName the cluster name.
   * @param customFields custom fields, {@link VirtualTopologyGroupConstants#GROUP_NAME}
   *                     and {@link VirtualTopologyGroupConstants#GROUP_NUMBER} are required,
   *                     {@link VirtualTopologyGroupConstants#AUTO_MAINTENANCE_MODE_DISABLED} is optional.
   *                     -- if set ture, the cluster will NOT automatically enter/exit maintenance mode during this API call;
   *                     -- if set false or not set, the cluster will automatically enter maintenance mode and exit after
   *                     the call succeeds. It won't proceed if the cluster is already in maintenance mode.
   *                     Either case, the cluster must be in maintenance mode before config change.
   *                     {@link VirtualTopologyGroupConstants#ASSIGNMENT_ALGORITHM_TYPE} is optional, default to INSTANCE_BASED.
   *                     {@link VirtualTopologyGroupConstants#FORCE_RECOMPUTE} is optional, default to false.
   *                     -- if set true, the virtual topology group will be recomputed from scratch by ignoring the existing
   *                     virtual topology group information.
   *                     -- if set false or not set, the virtual topology group will be incrementally computed based on the
   *                     existing virtual topology group information if possible.
   *                     {@link VirtualTopologyGroupConstants#MAX_IMBALANCE_THRESHOLD} is optional, default to -1.
   *                     -- if set to a non-negative value, the virtual topology group assignment will be checked for imbalance
   *                     and recomputed if the imbalance is detected.
   *                     -- if set to -1 or not set, the virtual topology group assignment will not be checked for imbalance.
   *                     {@link VirtualTopologyGroupConstants#IMBALANCE_DETECTION_ALGORITHM_TYPE} is optional, default to INSTANCE_COUNT_BASED.
   *                     -- if set to INSTANCE_COUNT_BASED, the imbalance detection will be based on the number of instances in each virtual group.
   *                     -- if set to INSTANCE_WEIGHT_BASED, the imbalance detection will be based on the weight of instances in each virtual group.
   */
  public void addVirtualTopologyGroup(String clusterName, Map<String, String> customFields) {
    // validation
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(clusterName);
    // Collect the real topology of the cluster and the virtual topology of the cluster
    ClusterTopology clusterTopology = _clusterService.getTopologyOfVirtualCluster(clusterName, true);
    Map<String, Set<String>> zoneMapping = clusterTopology.toZoneMapping();
    // If forceRecompute is set to true, we will recompute the virtual topology group from scratch
    // by ignoring the existing virtual topology group information.
    boolean forceRecompute = Boolean.parseBoolean(
        customFields.getOrDefault(VirtualTopologyGroupConstants.FORCE_RECOMPUTE, "false"));
    Map<String, Set<String>> existingVirtualZoneMapping = forceRecompute ? Collections.emptyMap()
        : _clusterService.getTopologyOfVirtualCluster(clusterName, false).toZoneMapping();

    Preconditions.checkState(clusterConfig.isTopologyAwareEnabled(),
        "Topology-aware rebalance is not enabled in cluster " + clusterName);
    final String groupName = customFields.get(VirtualTopologyGroupConstants.GROUP_NAME);
    final String groupNumberStr = customFields.get(VirtualTopologyGroupConstants.GROUP_NUMBER);
    Preconditions.checkArgument(!StringUtils.isEmpty(groupName), "virtualTopologyGroupName cannot be empty!");
    Preconditions.checkArgument(!StringUtils.isEmpty(groupNumberStr), "virtualTopologyGroupNumber cannot be empty!");
    int numGroups = 0;
    try {
      numGroups = Integer.parseInt(groupNumberStr);
      Preconditions.checkArgument(numGroups > 0, "Number of virtual groups should be positive.");
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("virtualTopologyGroupNumber " + groupNumberStr + " is not an integer.", ex);
    }

    // Default imbalance threshold is -1, which means no imbalance check.
    int imbalanceThreshold = DEFAULT_IMBALANCE_THRESHOLD_VALUE;
    try {
      if (customFields.get(MAX_IMBALANCE_THRESHOLD) != null) {
        imbalanceThreshold = Integer.parseInt(customFields.get(MAX_IMBALANCE_THRESHOLD));
      }
    } catch (NumberFormatException ex) {
      throw new IllegalArgumentException("imbalanceThreshold is not an integer.", ex);
    }

    // Update the assignment and imbalance detection algorithm based on the custom fields
    updateAssignmentAndImbalanceDetectAlgorithm(customFields, clusterTopology, numGroups);

    // compute group assignment
    LOG.info("Computing virtual topology group for cluster {} with param {}", clusterName, customFields);
    Map<String, Set<String>> assignment =
        _assignmentAlgorithm.computeAssignment(numGroups, groupName, zoneMapping,
            existingVirtualZoneMapping);

    // If recompute is not forced and the imbalance threshold is positive, we will check if the
    // assignment is imbalanced
    assignment = recomputeAssignmentIfNeeded(forceRecompute, imbalanceThreshold, assignment,
        numGroups, groupName, zoneMapping, existingVirtualZoneMapping);

    updateVirtualTopology(clusterName, clusterConfig, assignment, customFields, existingVirtualZoneMapping);
  }

  /**
   * Update the assignment algorithm based on the custom input and cluster topology.
   *
   * @param customFields custom fields that may contain the assignment algorithm type.
   * @param clusterTopology the cluster topology to determine the available zones and instances.
   * @param numGroups the number of virtual groups to be created.
   */
  private void updateAssignmentAndImbalanceDetectAlgorithm(Map<String, String> customFields,
      ClusterTopology clusterTopology, int numGroups) {
    String assignmentAlgorithm =
        customFields.get(VirtualTopologyGroupConstants.ASSIGNMENT_ALGORITHM_TYPE);
    assignmentAlgorithm = assignmentAlgorithm == null
        ? VirtualTopologyGroupConstants.VirtualGroupAssignmentAlgorithm.INSTANCE_BASED.toString()
        : assignmentAlgorithm;
    if (assignmentAlgorithm != null) {
      VirtualTopologyGroupConstants.VirtualGroupAssignmentAlgorithm algorithmEnum = null;
      try {
        algorithmEnum = VirtualTopologyGroupConstants.VirtualGroupAssignmentAlgorithm.valueOf(
            assignmentAlgorithm);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to instantiate assignment algorithm " + assignmentAlgorithm, e);
      }
      switch (algorithmEnum) {
        case ZONE_BASED:
          Preconditions.checkArgument(numGroups <= clusterTopology.getZones().size(),
              "Number of virtual groups cannot be greater than the number of zones.");
          _assignmentAlgorithm = FaultZoneBasedVirtualGroupAssignmentAlgorithm.getInstance();
          break;
        case INSTANCE_BASED:
          Preconditions.checkArgument(numGroups <= clusterTopology.getAllInstances().size(),
              "Number of virtual groups cannot be greater than the number of instances.");
          _assignmentAlgorithm = FifoVirtualGroupAssignmentAlgorithm.getInstance();
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported assignment algorithm " + assignmentAlgorithm);
      }
    }

    String imbalanceDetectionAlgorithm =
        customFields.get(VirtualTopologyGroupConstants.IMBALANCE_DETECTION_ALGORITHM_TYPE);
    if (imbalanceDetectionAlgorithm != null) {
      VirtualTopologyGroupConstants.VirtualGroupImbalanceDetectionAlgorithm imbalanceAlgorithmEnum =
          null;
      try {
        imbalanceAlgorithmEnum =
            VirtualTopologyGroupConstants.VirtualGroupImbalanceDetectionAlgorithm.valueOf(
                imbalanceDetectionAlgorithm);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            "Failed to instantiate imbalance detection algorithm " + imbalanceDetectionAlgorithm,
            e);
      }
      switch (imbalanceAlgorithmEnum) {
        case INSTANCE_COUNT_BASED:
          _imbalanceDetectionAlgorithm = InstanceCountImbalanceAlgorithm.getInstance();
          break;
        case INSTANCE_WEIGHT_BASED:
          _imbalanceDetectionAlgorithm =
              InstanceWeightImbalanceAlgorithm.getInstance(_configAccessor,
                  clusterTopology.getClusterId());
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported imbalance detection algorithm " + imbalanceDetectionAlgorithm);
      }
    }
  }

  /**
   * If recomputation isn’t forced and the imbalance threshold is positive,
   * evaluates whether a fresh assignment reduces the imbalance score.
   * If so, updates the assignment to the better one.
   */
  private Map<String, Set<String>> recomputeAssignmentIfNeeded(boolean forceRecompute,
      int threshold, Map<String, Set<String>> assignmentToUpdate, int numGroups, String groupName,
      Map<String, Set<String>> zoneMapping, Map<String, Set<String>> existingVirtualZoneMapping) {

    // Skip if forced recompute is requested elsewhere or threshold disables this check or the
    // existing virtual zone mapping size does not match the assignment to update size (meaning that
    // a fresh computation is applied)
    if (forceRecompute || threshold < 0 || existingVirtualZoneMapping.size() != assignmentToUpdate.size()) {
      return assignmentToUpdate;
    }

    int currentScore = _imbalanceDetectionAlgorithm.getImbalanceScore(assignmentToUpdate);
    if (currentScore <= threshold) {
      LOG.info("Assignment balanced (score {} ≤ threshold {})", currentScore, threshold);
      return assignmentToUpdate;
    }

    LOG.info("Imbalanced assignment (score {} > threshold {}), attempting recompute", currentScore,
        threshold);

    Map<String, Set<String>> candidateAssignment =
        _assignmentAlgorithm.computeAssignment(numGroups, groupName, zoneMapping,
            Collections.emptyMap());

    int candidateScore = _imbalanceDetectionAlgorithm.getImbalanceScore(candidateAssignment);
    if (candidateScore < currentScore) {
      LOG.info("Recompute successful: reduced score from {} to {}", currentScore, candidateScore);
      return candidateAssignment;
    }

    LOG.warn("Recomputed assignment worse ({} ≥ {}), retaining original", candidateScore,
        currentScore);
    return assignmentToUpdate;
  }

  private void updateVirtualTopology(String clusterName, ClusterConfig clusterConfig,
      Map<String, Set<String>> assignment, Map<String, String> customFields,
      Map<String, Set<String>> exisingVirtualZoneMapping) {
    boolean isAssignmentChanged = assignment.entrySet().stream().anyMatch(entry -> {
      String virtualGroup = entry.getKey();
      Set<String> instances = entry.getValue();
      if (instances.isEmpty()) {
        LOG.warn("Virtual group {} has no instances assigned, skipping.", virtualGroup);
        return false; // No change needed for empty groups
      }
      // Check if the group already exists in the virtual topology
      if (exisingVirtualZoneMapping.containsKey(virtualGroup)) {
        Set<String> existingInstances = exisingVirtualZoneMapping.get(virtualGroup);
        return !existingInstances.equals(instances); // Return true if there is a change
      }
      return true; // New group, so it is a change
    });

    // If the assignment is unchanged, we skip the update.
    if (!isAssignmentChanged) {
      LOG.info("Virtual topology group assignment is unchanged, skipping update for cluster {}", clusterName);
      return; // No change needed, skip the update
    }

    boolean autoMaintenanceModeDisabled = Boolean.parseBoolean(
        customFields.getOrDefault(VirtualTopologyGroupConstants.AUTO_MAINTENANCE_MODE_DISABLED, "false"));
    // if auto mode is NOT disabled, let service enter maintenance mode and exit after the API succeeds.
    if (!autoMaintenanceModeDisabled) {
      Preconditions.checkState(!_helixAdmin.isInMaintenanceMode(clusterName),
          "This operation is not allowed if cluster is already in maintenance mode before the API call. "
              + "Please set autoMaintenanceModeDisabled=true if this is intended.");
      _helixAdmin.manuallyEnableMaintenanceMode(clusterName, true,
          "Enable maintenanceMode for virtual topology group change.", customFields);
    }
    Preconditions.checkState(_helixAdmin.isInMaintenanceMode(clusterName),
        "Cluster is not in maintenance mode. This is required for virtual topology group setting. "
            + "Please set autoMaintenanceModeDisabled=false (default) to let the cluster enter maintenance mode automatically, "
            + "or use autoMaintenanceModeDisabled=true and control cluster maintenance mode in client side.");

    updateConfigs(clusterName, clusterConfig, assignment);
    if (!autoMaintenanceModeDisabled) {
      _helixAdmin.manuallyEnableMaintenanceMode(clusterName, false,
          "Disable maintenanceMode after virtual topology group change.", customFields);
    }
  }

  private void updateConfigs(String clusterName, ClusterConfig clusterConfig, Map<String, Set<String>> assignment) {
    List<String> zkPaths = new ArrayList<>();
    List<DataUpdater<ZNRecord>> updaters = new ArrayList<>();
    createInstanceConfigUpdater(clusterConfig, assignment).forEach((zkPath, updater) -> {
      zkPaths.add(zkPath);
      updaters.add(updater);
    });
    // update instance config
    boolean[] results = _dataAccessor.updateChildren(zkPaths, updaters, AccessOption.EPHEMERAL);
    for (int i = 0; i < results.length; i++) {
      if (!results[i]) {
        throw new HelixException("Failed to update instance config for path " + zkPaths.get(i));
      }
    }
    // update cluster config
    String virtualTopologyString = computeVirtualTopologyString(clusterConfig);
    clusterConfig.setTopology(virtualTopologyString);
    clusterConfig.setFaultZoneType(computeVirtualFaultZoneTypeKey(clusterConfig.getFaultZoneType()));
    _configAccessor.updateClusterConfig(clusterName, clusterConfig);
    LOG.info("Successfully update instance and cluster config for {}", clusterName);
  }

  /**
   * Compute the virtual topology string based on the cluster config by replacing the old fault zone
   * with the new virtual topology fault zone key.
   *
   * @param clusterConfig cluster config for the cluster.
   * @return the updated virtual topology string.
   */
  @VisibleForTesting
  static String computeVirtualTopologyString(ClusterConfig clusterConfig) {
    ClusterTopologyConfig clusterTopologyConfig =
        ClusterTopologyConfig.createFromClusterConfig(clusterConfig);
    String topologyString = clusterConfig.getTopology();

    if (topologyString == null || topologyString.isEmpty()) {
      throw new IllegalArgumentException("Topology string cannot be null or empty");
    }

    String newFaultZone = computeVirtualFaultZoneTypeKey(clusterConfig.getFaultZoneType());
    String[] newTopologyString = topologyString.split(PATH_NAME_SPLITTER);
    for (int i = 0; i < newTopologyString.length; i++) {
      if (newTopologyString[i].equals(clusterTopologyConfig.getFaultZoneType())) {
        newTopologyString[i] = newFaultZone;
      }
    }
    return String.join(PATH_NAME_SPLITTER, newTopologyString);
  }

  /**
   * Create updater for instance config for async update.
   * @param clusterConfig cluster config for the cluster which the instance reside.
   * @param assignment virtual group assignment.
   * @return a map from instance zkPath to its {@link DataUpdater} to update.
   */
  @VisibleForTesting
  static Map<String, DataUpdater<ZNRecord>> createInstanceConfigUpdater(
      ClusterConfig clusterConfig, Map<String, Set<String>> assignment) {
    Map<String, DataUpdater<ZNRecord>> updaters = new HashMap<>();
    for (Map.Entry<String, Set<String>> entry : assignment.entrySet()) {
      String virtualGroup = entry.getKey();
      for (String instanceName : entry.getValue()) {
        String path = PropertyPathBuilder.instanceConfig(clusterConfig.getClusterName(), instanceName);
        updaters.put(path, currentData -> {
          InstanceConfig instanceConfig = new InstanceConfig(currentData);
          Map<String, String> domainMap = instanceConfig.getDomainAsMap();
          domainMap.put(computeVirtualFaultZoneTypeKey(clusterConfig.getFaultZoneType()), virtualGroup);
          instanceConfig.setDomain(domainMap);
          return instanceConfig.getRecord();
        });
      }
    }
    return updaters;
  }
}
