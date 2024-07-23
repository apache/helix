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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class ConditionBasedRebalancer extends AbstractRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(ConditionBasedRebalancer.class);
  private final RebalanceCondition _rebalanceCondition;

  public ConditionBasedRebalancer(RebalanceCondition rebalanceCondition) {
    this._rebalanceCondition = rebalanceCondition;
  }

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ResourceControllerDataProvider clusterData) {
    if (!this._rebalanceCondition.evaluate()) {
      ZNRecord cachedIdealState = clusterData.getCachedOndemandIdealState(resourceName);
      if (cachedIdealState != null) {
        return new IdealState(cachedIdealState);
      }
      // In theory, the cache should be populated already if no rebalance is needed
      LOG.warn(String.format(
          "Cannot fetch the cached Ideal State for resource: %s, will recompute the Ideal State",
          resourceName));
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
    List<String> assignableLiveNodes = new ArrayList<>(assignableLiveInstance.keySet());
    List<String> assignableNodes = new ArrayList<>(clusterData.getAssignableInstances());
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
      assignableNodes = new ArrayList<>(taggedNodes);
      assignableLiveNodes = new ArrayList<>(taggedLiveNodes);
    }

    // sort node lists to ensure consistent preferred assignments
    Collections.sort(assignableNodes);
    Collections.sort(assignableLiveNodes);

    int maxPartition = currentIdealState.getMaxPartitionsPerInstance();
    _rebalanceStrategy =
        getRebalanceStrategy(currentIdealState.getRebalanceStrategy(), partitions, resourceName,
            stateCountMap, maxPartition);
    ZNRecord newMapping =
        _rebalanceStrategy.computePartitionAssignment(assignableNodes, assignableLiveNodes,
            currentMapping, clusterData);

    LOG.debug("currentMapping: {}", currentMapping);
    LOG.debug("stateCountMap: {}", stateCountMap);
    LOG.debug("assignableLiveNodes: {}", assignableLiveNodes);
    LOG.debug("assignableNodes: {}", assignableNodes);
    LOG.debug("maxPartition: {}", maxPartition);
    LOG.debug("newMapping: {}", newMapping);

    IdealState newIdealState = new IdealState(resourceName);
    newIdealState.getRecord().setSimpleFields(currentIdealState.getRecord().getSimpleFields());
    newIdealState.setRebalanceMode(IdealState.RebalanceMode.FULL_AUTO);
    newIdealState.getRecord().setListFields(newMapping.getListFields());

    clusterData.setCachedOndemandIdealState(resourceName, newIdealState.getRecord());

    return newIdealState;
  }

  @Override
  public ResourceAssignment computeBestPossiblePartitionState(ResourceControllerDataProvider cache,
      IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput) {
    ZNRecord cachedIdealState = cache.getCachedOndemandIdealState(resource.getResourceName());
    if (!this._rebalanceCondition.evaluate()) {
      if (cachedIdealState != null && cachedIdealState.getMapFields() != null) {
        ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
        for (Partition partition : resource.getPartitions()) {
          partitionMapping.addReplicaMap(partition, cachedIdealState.getMapFields().get(partition));
        }
        return new ResourceAssignment(cachedIdealState);
      }
      // In theory, the cache should be populated already if no rebalance is needed
      LOG.warn(String.format(
          "Cannot fetch the cached assignment for resource: %s, will recompute the assignment",
          resource.getResourceName()));
    }

    LOG.info("Computing BestPossibleMapping for " + resource.getResourceName());

    // TODO: Change the logic to apply different assignment strategy
    ResourceAssignment assignment =
        super.computeBestPossiblePartitionState(cache, idealState, resource, currentStateOutput);
    // Cache the assignment so no need to recompute the result next time
    cachedIdealState.setMapFields(assignment.getRecord().getMapFields());
    cache.setCachedOndemandIdealState(resource.getResourceName(), cachedIdealState);

    return assignment;
  }
}
