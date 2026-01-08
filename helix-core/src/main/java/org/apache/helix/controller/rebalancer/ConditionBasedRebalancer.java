package org.apache.helix.controller.rebalancer;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.helix.HelixException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.condition.RebalanceCondition;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code ConditionBasedRebalancer} class extends the {@link AbstractRebalancer} and
 * perform the rebalance operation based on specific list of conditions defined by the
 * {@link RebalanceCondition} interface.
 */
public class ConditionBasedRebalancer extends AbstractRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionBasedRebalancer.class);
  private final List<RebalanceCondition> _rebalanceConditions;

  public ConditionBasedRebalancer() {
    this._rebalanceConditions = new ArrayList<>();
  }

  public ConditionBasedRebalancer(List<RebalanceCondition> rebalanceConditions) {
    this._rebalanceConditions = rebalanceConditions;
  }

  /**
   * Compute new Ideal State iff all conditions are met, otherwise just return from cached Ideal State
   *
   * @param resourceName        the name of the resource for which to compute the new ideal state.
   * @param currentIdealState   the current {@link IdealState} of the resource.
   * @param currentStateOutput  the current state output, containing the actual states of the
   *                            partitions.
   * @param clusterData         the {@link ResourceControllerDataProvider} instance providing
   *                            additional data required for the computation.
   * @return the newly computed {@link IdealState} for the resource.
   */
  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    ZNRecord cachedIdealState = clusterData.getCachedOndemandIdealState(resourceName);
    // If previous placement list exists in cache && all condition met -> return cached value
    if (cachedIdealState != null && cachedIdealState.getListFields() != null
        && !cachedIdealState.getListFields().isEmpty() && !this._rebalanceConditions.stream()
        .allMatch(condition -> condition.shouldPerformRebalance(clusterData))) {
      return new IdealState(cachedIdealState);
    }

    LOG.info("Computing IdealState for " + resourceName);

    List<String> partitions = getStablePartitionList(clusterData, currentIdealState);
    String stateModelName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
    if (stateModelDef == null) {
      LOG.error("State Model Definition null for resource: " + resourceName);
      throw new HelixException("State Model Definition null for resource: " + resourceName);
    }
    Map<String, LiveInstance> assignableLiveInstance = clusterData.getAssignableLiveInstances();
    int replicas = currentIdealState.getReplicaCount(assignableLiveInstance.size());

    LinkedHashMap<String, Integer> stateCountMap =
        stateModelDef.getStateCountMap(assignableLiveInstance.size(), replicas);
    Set<String> assignableLiveNodes = new HashSet<>(assignableLiveInstance.keySet());
    Set<String> assignableNodes = new HashSet<>(clusterData.getAssignableInstances());
    assignableNodes.removeAll(clusterData.getDisabledInstances());
    assignableLiveNodes.retainAll(assignableNodes);

    Map<String, Map<String, String>> currentMapping =
        currentMapping(currentStateOutput, resourceName, partitions, stateCountMap);

    // If there are nodes tagged with resource name, use only those nodes
    Set<String> taggedNodes = new HashSet<String>();
    Set<String> taggedLiveNodes = new HashSet<String>();
    if (currentIdealState.getInstanceGroupTag() != null) {
      for (String instanceName : assignableNodes) {
        if (clusterData.getAssignableInstanceConfigMap().get(instanceName)
            .containsTag(currentIdealState.getInstanceGroupTag())) {
          taggedNodes.add(instanceName);
          if (assignableLiveInstance.containsKey(instanceName)) {
            taggedLiveNodes.add(instanceName);
          }
        }
      }
      if (!taggedLiveNodes.isEmpty()) {
        // live nodes exist that have this tag
        if (LOG.isInfoEnabled()) {
          LOG.info(
              "found the following participants with tag " + currentIdealState.getInstanceGroupTag()
                  + " for " + resourceName + ": " + taggedLiveNodes);
        }
      } else if (taggedNodes.isEmpty()) {
        // no live nodes and no configured nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no configured participants have this tag");
      } else {
        // configured nodes have this tag, but no live nodes have this tag
        LOG.warn("Resource " + resourceName + " has tag " + currentIdealState.getInstanceGroupTag()
            + " but no live participants have this tag");
      }
      assignableNodes = new HashSet<>(taggedNodes);
      assignableLiveNodes = new HashSet<>(taggedLiveNodes);
    }

    // sort node lists to ensure consistent preferred assignments
    List<String> assignableNodesList =
        assignableNodes.stream().sorted().collect(Collectors.toList());
    List<String> assignableLiveNodesList =
        assignableLiveNodes.stream().sorted().collect(Collectors.toList());

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), partitions, resourceName,
            stateCountMap, maxPartition);
    ZNRecord newMapping =
        _rebalanceStrategy.computePartitionAssignment(assignableNodesList, assignableLiveNodesList,
            currentMapping, clusterData);

    if (LOG.isDebugEnabled()) {
      LOG.debug("currentMapping: {}", currentMapping);
      LOG.debug("stateCountMap: {}", stateCountMap);
      LOG.debug("assignableLiveNodes: {}", assignableLiveNodes);
      LOG.debug("assignableNodes: {}", assignableNodes);
      LOG.debug("maxPartition: {}", maxPartition);
      LOG.debug("newMapping: {}", newMapping);
    }

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    newIdealState.getRecord().setListFields(newMapping.getListFields());

    clusterData.setCachedOndemandIdealState(resourceName, newIdealState.getRecord());

    return newIdealState;
  }

  /**
   * Compute new assignment iff all conditions are met, otherwise just return from cached assignment
   *
   * @param cache               the {@link ResourceControllerDataProvider} instance providing
   *                            metadata and state information about the cluster.
   * @param idealState          the {@link IdealState} representing the current ideal state.
   * @param resource            the {@link Resource} for which to compute the best possible partition
   *                            state.
   * @param currentStateOutput  the {@link CurrentStateOutput} containing the current states of the
   *                            partitions.
   * @return the {@link ResourceAssignment} representing the best possible state assignment for the
   *         partitions of the resource.
   */
  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ResourceControllerDataProvider cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    ZNRecord cachedIdealState = cache.getCachedOndemandIdealState(resource.getResourceName());
    // If previous assignment map exists in cache && all condition met -> return cached value
    if (cachedIdealState.getMapFields() != null && !cachedIdealState.getMapFields().isEmpty()
        && !this._rebalanceConditions.stream()
        .allMatch(condition -> condition.shouldPerformRebalance(cache))) {
      ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
      for (Partition partition : resource.getPartitions()) {
        partitionMapping.addReplicaMap(partition,
            cachedIdealState.getMapFields().get(partition.getPartitionName()));
      }
      return partitionMapping;
    }

    LOG.info("Computing BestPossibleMapping for " + resource.getResourceName());

    // TODO: Change the logic to apply different assignment strategy
    ResourceAssignment assignment =
        super.computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
    // Cache the assignment so no need to recompute the result next time
    cachedIdealState.setMapFields(assignment.getRecord().getMapFields());
    cache.setCachedOndemandIdealState(resource.getResourceName(), cachedIdealState);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Processed resource: {}", resource.getResourceName());
      LOG.debug("Final Mapping of resource : {}", assignment);
    }
    return assignment;
  }
}
