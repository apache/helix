package org.apache.helix.api;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.ClusterConfiguration;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Message;
import org.apache.helix.model.PauseSignal;
import org.apache.log4j.Logger;

public class ClusterAccessor {
  private static Logger LOG = Logger.getLogger(ClusterAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final ClusterId _clusterId;

  public ClusterAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
    _clusterId = clusterId;
  }

  /**
   * create a new cluster, fail if it already exists
   * @return true if created, false if creation failed
   */
  public boolean createCluster(ClusterConfig cluster) {
    boolean created = _accessor.createProperty(_keyBuilder.cluster(), null);
    if (!created) {
      LOG.warn("Cluster already created. Aborting.");
      return false;
    }
    Map<ResourceId, ResourceConfig> resources = cluster.getResourceMap();
    for (ResourceConfig resource : resources.values()) {
      addResourceToCluster(resource);
    }
    Map<ParticipantId, ParticipantConfig> participants = cluster.getParticipantMap();
    for (ParticipantConfig participant : participants.values()) {
      addParticipantToCluster(participant);
    }
    _accessor.createProperty(_keyBuilder.constraints(), null);
    for (ClusterConstraints constraints : cluster.getConstraintMap().values()) {
      _accessor.createProperty(_keyBuilder.constraint(constraints.getType().toString()),
          constraints);
    }
    _accessor
        .createProperty(_keyBuilder.clusterConfig(), new ClusterConfiguration(cluster.getId()));
    if (cluster.isPaused()) {
      pauseCluster();
    }

    return true;
  }

  /**
   * drop a cluster
   */
  public void dropCluster() {
    LOG.info("Dropping cluster: " + _clusterId);
    List<String> liveInstanceNames = _accessor.getChildNames(_keyBuilder.liveInstances());
    if (liveInstanceNames.size() > 0) {
      throw new HelixException("Can't drop cluster: " + _clusterId
          + " because there are running participant: " + liveInstanceNames
          + ", shutdown participants first.");
    }

    LiveInstance leader = _accessor.getProperty(_keyBuilder.controllerLeader());
    if (leader != null) {
      throw new HelixException("Can't drop cluster: " + _clusterId + ", because leader: "
          + leader.getId() + " are running, shutdown leader first.");
    }

    _accessor.removeProperty(_keyBuilder.cluster());
  }

  /**
   * read entire cluster data
   * @return cluster
   */
  public Cluster readCluster() {
    // TODO many of these should live in resource, participant, etc accessors
    /**
     * map of instance-id to instance-config
     */
    Map<String, InstanceConfig> instanceConfigMap =
        _accessor.getChildValuesMap(_keyBuilder.instanceConfigs());

    /**
     * map of resource-id to ideal-state
     */
    Map<String, IdealState> idealStateMap = _accessor.getChildValuesMap(_keyBuilder.idealStates());

    /**
     * map of instance-id to live-instance
     */
    Map<String, LiveInstance> liveInstanceMap =
        _accessor.getChildValuesMap(_keyBuilder.liveInstances());

    /**
     * map of participant-id to map of message-id to message
     */
    Map<String, Map<String, Message>> messageMap = new HashMap<String, Map<String, Message>>();
    for (String instanceName : liveInstanceMap.keySet()) {
      Map<String, Message> instanceMsgMap =
          _accessor.getChildValuesMap(_keyBuilder.messages(instanceName));
      messageMap.put(instanceName, instanceMsgMap);
    }

    /**
     * map of participant-id to map of resource-id to current-state
     */
    Map<String, Map<String, CurrentState>> currentStateMap =
        new HashMap<String, Map<String, CurrentState>>();
    for (String participantName : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(participantName);
      SessionId sessionId = liveInstance.getSessionId();
      Map<String, CurrentState> instanceCurStateMap =
          _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
              sessionId.stringify()));

      currentStateMap.put(participantName, instanceCurStateMap);
    }

    LiveInstance leader = _accessor.getProperty(_keyBuilder.controllerLeader());

    /**
     * map of constraint-type to constraints
     */
    Map<String, ClusterConstraints> constraintMap =
        _accessor.getChildValuesMap(_keyBuilder.constraints());

    /**
     * Map of resource id to external view
     */
    Map<String, ExternalView> externalViewMap =
        _accessor.getChildValuesMap(_keyBuilder.externalViews());

    Map<ResourceId, Resource> resourceMap = new HashMap<ResourceId, Resource>();
    for (String resourceName : idealStateMap.keySet()) {
      IdealState idealState = idealStateMap.get(resourceName);
      // TODO pass resource assignment
      ResourceId resourceId = Id.resource(resourceName);
      resourceMap.put(resourceId,
          new Resource(resourceId, idealState, null, externalViewMap.get(resourceName),
              liveInstanceMap.size()));
    }

    Map<ParticipantId, Participant> participantMap = new HashMap<ParticipantId, Participant>();
    for (String participantName : instanceConfigMap.keySet()) {
      InstanceConfig instanceConfig = instanceConfigMap.get(participantName);
      LiveInstance liveInstance = liveInstanceMap.get(participantName);
      Map<String, Message> instanceMsgMap = messageMap.get(participantName);

      ParticipantId participantId = new ParticipantId(participantName);

      participantMap.put(participantId, ParticipantAccessor.createParticipant(participantId,
          instanceConfig, liveInstance, instanceMsgMap, currentStateMap.get(participantName)));
    }

    Map<ControllerId, Controller> controllerMap = new HashMap<ControllerId, Controller>();
    ControllerId leaderId = null;
    if (leader != null) {
      leaderId = new ControllerId(leader.getId());
      controllerMap.put(leaderId, new Controller(leaderId, leader, true));
    }

    Map<ConstraintType, ClusterConstraints> clusterConstraintMap =
        new HashMap<ConstraintType, ClusterConstraints>();
    for (String constraintType : constraintMap.keySet()) {
      clusterConstraintMap.put(ConstraintType.valueOf(constraintType),
          constraintMap.get(constraintType));
    }

    PauseSignal pauseSignal = _accessor.getProperty(_keyBuilder.pause());
    boolean isPaused = pauseSignal != null;

    return new Cluster(_clusterId, resourceMap, participantMap, controllerMap, leaderId,
        clusterConstraintMap, isPaused);
  }

  /**
   * pause controller of cluster
   */
  public void pauseCluster() {
    _accessor.createProperty(_keyBuilder.pause(), new PauseSignal("pause"));
  }

  /**
   * resume controller of cluster
   */
  public void resumeCluster() {
    _accessor.removeProperty(_keyBuilder.pause());
  }

  /**
   * add a resource to cluster
   * @param resource
   */
  public void addResourceToCluster(ResourceConfig resource) {
    StateModelDefId stateModelDefId = resource.getRebalancerConfig().getStateModelDefId();
    if (_accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify())) == null) {
      throw new HelixException("State model: " + stateModelDefId + " not found in cluster: "
          + _clusterId);
    }

    ResourceId resourceId = resource.getId();
    if (_accessor.getProperty(_keyBuilder.idealState(resourceId.stringify())) != null) {
      throw new HelixException("Skip adding resource: " + resourceId
          + ", because resource ideal state already exists in cluster: " + _clusterId);
    }

    // Create an IdealState from a RebalancerConfig
    RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();
    IdealState idealState = new IdealState(resourceId);
    idealState.setRebalanceMode(rebalancerConfig.getRebalancerMode());
    idealState.setMaxPartitionsPerInstance(rebalancerConfig.getMaxPartitionsPerParticipant());
    if (rebalancerConfig.canAssignAnyLiveParticipant()) {
      idealState.setReplicas(HelixConstants.StateModelToken.ANY_LIVEINSTANCE.toString());
    } else {
      idealState.setReplicas(Integer.toString(rebalancerConfig.getReplicaCount()));
    }
    idealState.setStateModelDefId(rebalancerConfig.getStateModelDefId());
    for (PartitionId partitionId : resource.getPartitionSet()) {
      List<ParticipantId> preferenceList = rebalancerConfig.getPreferenceList(partitionId);
      Map<ParticipantId, State> preferenceMap = rebalancerConfig.getPreferenceMap(partitionId);
      if (preferenceList != null) {
        idealState.setPreferenceList(partitionId, preferenceList);
      }
      if (preferenceMap != null) {
        idealState.setParticipantStateMap(partitionId, preferenceMap);
      }
    }
    idealState.setBucketSize(resource.getBucketSize());
    idealState.setBatchMessageMode(resource.getBatchMessageMode());
    String groupTag = rebalancerConfig.getParticipantGroupTag();
    if (groupTag != null) {
      idealState.setInstanceGroupTag(groupTag);
    }
    RebalancerRef rebalancerRef = rebalancerConfig.getRebalancerRef();
    if (rebalancerRef != null) {
      idealState.setRebalancerRef(rebalancerRef);
    }
    StateModelFactoryId stateModelFactoryId = rebalancerConfig.getStateModelFactoryId();
    if (stateModelFactoryId != null) {
      idealState.setStateModelFactoryId(stateModelFactoryId);
    }
    _accessor.createProperty(_keyBuilder.idealState(resourceId.stringify()), idealState);
  }

  /**
   * drop a resource from cluster
   * @param resourceId
   */
  public void dropResourceFromCluster(ResourceId resourceId) {
    _accessor.removeProperty(_keyBuilder.idealState(resourceId.stringify()));
    _accessor.removeProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
  }

  /**
   * check if cluster structure is valid
   * @return true if valid or false otherwise
   */
  public boolean isClusterStructureValid() {
    // TODO impl this
    return true;
  }

  /**
   * add a participant to cluster
   * @param participant
   */
  public void addParticipantToCluster(ParticipantConfig participant) {
    if (!isClusterStructureValid()) {
      throw new HelixException("Cluster: " + _clusterId + " structure is not valid");
    }

    ParticipantId participantId = participant.getId();
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) != null) {
      throw new HelixException("Config for participant: " + participantId
          + " already exists in cluster: " + _clusterId);
    }

    // add empty root ZNodes
    List<PropertyKey> createKeys = new ArrayList<PropertyKey>();
    createKeys.add(_keyBuilder.messages(participantId.stringify()));
    createKeys.add(_keyBuilder.currentStates(participantId.stringify()));
    createKeys.add(_keyBuilder.participantErrors(participantId.stringify()));
    createKeys.add(_keyBuilder.statusUpdates(participantId.stringify()));
    for (PropertyKey key : createKeys) {
      _accessor.createProperty(key, null);
    }

    // add the config
    InstanceConfig instanceConfig = new InstanceConfig(participant.getId());
    instanceConfig.setHostName(participant.getHostName());
    instanceConfig.setPort(Integer.toString(participant.getPort()));
    instanceConfig.setInstanceEnabled(participant.isEnabled());
    Set<String> tags = participant.getTags();
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    Set<PartitionId> disabledPartitions = participant.getDisablePartitionIds();
    for (PartitionId partitionId : disabledPartitions) {
      instanceConfig.setInstanceEnabledForPartition(partitionId, false);
    }
    _accessor.createProperty(_keyBuilder.instanceConfig(participantId.stringify()), instanceConfig);
    _accessor.createProperty(_keyBuilder.messages(participantId.stringify()), null);
  }

  /**
   * drop a participant from cluster
   * @param participantId
   */
  public void dropParticipantFromCluster(ParticipantId participantId) {
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) == null) {
      throw new HelixException("Config for participant: " + participantId
          + " does NOT exist in cluster: " + _clusterId);
    }

    if (_accessor.getProperty(_keyBuilder.instance(participantId.stringify())) == null) {
      throw new HelixException("Participant: " + participantId
          + " structure does NOT exist in cluster: " + _clusterId);
    }

    // delete participant config path
    _accessor.removeProperty(_keyBuilder.instanceConfig(participantId.stringify()));

    // delete participant path
    _accessor.removeProperty(_keyBuilder.instance(participantId.stringify()));
  }
}
