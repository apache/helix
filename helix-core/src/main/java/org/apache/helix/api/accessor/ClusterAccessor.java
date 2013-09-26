package org.apache.helix.api.accessor;

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

import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Controller;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
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
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.StateModelDefinition;
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
      LOG.error("Cluster already created. Aborting.");
      return false;
    }
    initClusterStructure();
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = cluster.getStateModelMap();
    for (StateModelDefinition stateModelDef : stateModelDefs.values()) {
      addStateModelDefinitionToCluster(stateModelDef);
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
    _accessor.createProperty(_keyBuilder.clusterConfig(),
        ClusterConfiguration.from(cluster.getUserConfig()));
    if (cluster.isPaused()) {
      pauseCluster();
    }

    return true;
  }

  /**
   * Update the cluster configuration
   * @param clusterDelta change to the cluster configuration
   * @return updated ClusterConfig, or null if there was an error
   */
  public ClusterConfig updateCluster(ClusterConfig.Delta clusterDelta) {
    Cluster cluster = readCluster();
    if (cluster == null) {
      LOG.error("Cluster does not exist, cannot be updated");
      return null;
    }
    ClusterConfig config = clusterDelta.mergeInto(cluster.getConfig());
    boolean status = setBasicClusterConfig(config);
    return status ? config : null;
  }

  /**
   * Set a cluster config minus state model, participants, and resources
   * @param config ClusterConfig
   * @return true if correctly set, false otherwise
   */
  private boolean setBasicClusterConfig(ClusterConfig config) {
    if (config == null) {
      return false;
    }
    ClusterConfiguration configuration = ClusterConfiguration.from(config.getUserConfig());
    _accessor.setProperty(_keyBuilder.clusterConfig(), configuration);
    Map<ConstraintType, ClusterConstraints> constraints = config.getConstraintMap();
    for (ConstraintType type : constraints.keySet()) {
      ClusterConstraints constraint = constraints.get(type);
      _accessor.setProperty(_keyBuilder.constraint(type.toString()), constraint);
    }
    return true;
  }

  /**
   * drop a cluster
   * @return true if the cluster was dropped, false if there was an error
   */
  public boolean dropCluster() {
    LOG.info("Dropping cluster: " + _clusterId);
    List<String> liveInstanceNames = _accessor.getChildNames(_keyBuilder.liveInstances());
    if (liveInstanceNames.size() > 0) {
      LOG.error("Can't drop cluster: " + _clusterId + " because there are running participant: "
          + liveInstanceNames + ", shutdown participants first.");
      return false;
    }

    LiveInstance leader = _accessor.getProperty(_keyBuilder.controllerLeader());
    if (leader != null) {
      LOG.error("Can't drop cluster: " + _clusterId + ", because leader: " + leader.getId()
          + " are running, shutdown leader first.");
      return false;
    }

    return _accessor.removeProperty(_keyBuilder.cluster());
  }

  /**
   * read entire cluster data
   * @return cluster snapshot
   */
  public Cluster readCluster() {
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

    /**
     * Map of resource id to user configuration
     */
    Map<String, ResourceConfiguration> resourceConfigMap =
        _accessor.getChildValuesMap(_keyBuilder.resourceConfigs());

    /**
     * Map of resource id to resource assignment
     */
    Map<String, ResourceAssignment> resourceAssignmentMap =
        _accessor.getChildValuesMap(_keyBuilder.resourceAssignments());

    // read all the resources
    Map<ResourceId, Resource> resourceMap = new HashMap<ResourceId, Resource>();
    for (String resourceName : idealStateMap.keySet()) {
      ResourceId resourceId = ResourceId.from(resourceName);
      resourceMap.put(resourceId, ResourceAccessor.createResource(resourceId,
          resourceConfigMap.get(resourceName), idealStateMap.get(resourceName),
          externalViewMap.get(resourceName), resourceAssignmentMap.get(resourceName)));
    }

    // read all the participants
    Map<ParticipantId, Participant> participantMap = new HashMap<ParticipantId, Participant>();
    for (String participantName : instanceConfigMap.keySet()) {
      InstanceConfig instanceConfig = instanceConfigMap.get(participantName);
      UserConfig userConfig = UserConfig.from(instanceConfig);
      LiveInstance liveInstance = liveInstanceMap.get(participantName);
      Map<String, Message> instanceMsgMap = messageMap.get(participantName);

      ParticipantId participantId = ParticipantId.from(participantName);

      participantMap.put(participantId, ParticipantAccessor.createParticipant(participantId,
          instanceConfig, userConfig, liveInstance, instanceMsgMap,
          currentStateMap.get(participantName)));
    }

    // read the controllers
    Map<ControllerId, Controller> controllerMap = new HashMap<ControllerId, Controller>();
    ControllerId leaderId = null;
    if (leader != null) {
      leaderId = ControllerId.from(leader.getId());
      controllerMap.put(leaderId, new Controller(leaderId, leader, true));
    }

    // read the constraints
    Map<ConstraintType, ClusterConstraints> clusterConstraintMap =
        new HashMap<ConstraintType, ClusterConstraints>();
    for (String constraintType : constraintMap.keySet()) {
      clusterConstraintMap.put(ConstraintType.valueOf(constraintType),
          constraintMap.get(constraintType));
    }

    // read the pause status
    PauseSignal pauseSignal = _accessor.getProperty(_keyBuilder.pause());
    boolean isPaused = pauseSignal != null;

    ClusterConfiguration clusterUserConfig = _accessor.getProperty(_keyBuilder.clusterConfig());
    UserConfig userConfig;
    if (clusterUserConfig != null) {
      userConfig = UserConfig.from(clusterUserConfig);
    } else {
      userConfig = new UserConfig(Scope.cluster(_clusterId));
    }

    // read the state model definitions
    StateModelDefinitionAccessor stateModelDefAccessor =
        new StateModelDefinitionAccessor(_accessor);
    Map<StateModelDefId, StateModelDefinition> stateModelMap =
        stateModelDefAccessor.readStateModelDefinitions();

    // create the cluster snapshot object
    return new Cluster(_clusterId, resourceMap, participantMap, controllerMap, leaderId,
        clusterConstraintMap, stateModelMap, userConfig, isPaused);
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
   * @return true if resource added, false if there was an error
   */
  public boolean addResourceToCluster(ResourceConfig resource) {
    if (resource == null || resource.getRebalancerConfig() == null) {
      LOG.error("Resource not fully defined with a rebalancer context");
      return false;
    }

    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }
    RebalancerContext context =
        resource.getRebalancerConfig().getRebalancerContext(RebalancerContext.class);
    StateModelDefId stateModelDefId = context.getStateModelDefId();
    if (_accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify())) == null) {
      LOG.error("State model: " + stateModelDefId + " not found in cluster: " + _clusterId);
      return false;
    }

    ResourceId resourceId = resource.getId();
    if (_accessor.getProperty(_keyBuilder.idealState(resourceId.stringify())) != null) {
      LOG.error("Skip adding resource: " + resourceId
          + ", because resource ideal state already exists in cluster: " + _clusterId);
      return false;
    }
    if (_accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify())) != null) {
      LOG.error("Skip adding resource: " + resourceId
          + ", because resource config already exists in cluster: " + _clusterId);
      return false;
    }

    // Add resource user config
    if (resource.getUserConfig() != null) {
      ResourceConfiguration configuration = new ResourceConfiguration(resourceId);
      configuration.setType(resource.getType());
      configuration.addNamespacedConfig(resource.getUserConfig());
      configuration.addNamespacedConfig(resource.getRebalancerConfig().toNamespacedConfig());
      configuration.setBucketSize(resource.getBucketSize());
      configuration.setBatchMessageMode(resource.getBatchMessageMode());
      _accessor.createProperty(_keyBuilder.resourceConfig(resourceId.stringify()), configuration);
    }

    // Create an IdealState from a RebalancerConfig (if the resource is partitioned)
    RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();
    IdealState idealState =
        ResourceAccessor.rebalancerConfigToIdealState(rebalancerConfig, resource.getBucketSize(),
            resource.getBatchMessageMode());
    if (idealState != null) {
      _accessor.createProperty(_keyBuilder.idealState(resourceId.stringify()), idealState);
    }
    return true;
  }

  /**
   * drop a resource from cluster
   * @param resourceId
   * @return true if removal succeeded, false otherwise
   */
  public boolean dropResourceFromCluster(ResourceId resourceId) {
    if (_accessor.getProperty(_keyBuilder.idealState(resourceId.stringify())) == null) {
      LOG.error("Skip removing resource: " + resourceId
          + ", because resource ideal state already removed from cluster: " + _clusterId);
      return false;
    }
    _accessor.removeProperty(_keyBuilder.idealState(resourceId.stringify()));
    _accessor.removeProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return true;
  }

  /**
   * check if cluster structure is valid
   * @return true if valid or false otherwise
   */
  public boolean isClusterStructureValid() {
    List<String> paths = getRequiredPaths();
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    if (baseAccessor != null) {
      boolean[] existsResults = baseAccessor.exists(paths, 0);
      for (boolean exists : existsResults) {
        if (!exists) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Create empty persistent properties to ensure that there is a valid cluster structure
   */
  private void initClusterStructure() {
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    List<String> paths = getRequiredPaths();
    for (String path : paths) {
      boolean status = baseAccessor.create(path, null, AccessOption.PERSISTENT);
      if (!status && LOG.isDebugEnabled()) {
        LOG.debug(path + " already exists");
      }
    }
  }

  /**
   * Get all property paths that must be set for a cluster structure to be valid
   * @return list of paths as strings
   */
  private List<String> getRequiredPaths() {
    List<String> paths = new ArrayList<String>();
    paths.add(_keyBuilder.cluster().getPath());
    paths.add(_keyBuilder.idealStates().getPath());
    paths.add(_keyBuilder.clusterConfigs().getPath());
    paths.add(_keyBuilder.instanceConfigs().getPath());
    paths.add(_keyBuilder.resourceConfigs().getPath());
    paths.add(_keyBuilder.propertyStore().getPath());
    paths.add(_keyBuilder.liveInstances().getPath());
    paths.add(_keyBuilder.instances().getPath());
    paths.add(_keyBuilder.externalViews().getPath());
    paths.add(_keyBuilder.controller().getPath());
    paths.add(_keyBuilder.stateModelDefs().getPath());
    paths.add(_keyBuilder.controllerMessages().getPath());
    paths.add(_keyBuilder.controllerTaskErrors().getPath());
    paths.add(_keyBuilder.controllerTaskStatuses().getPath());
    paths.add(_keyBuilder.controllerLeaderHistory().getPath());
    return paths;
  }

  /**
   * add a participant to cluster
   * @param participant
   * @return true if participant added, false otherwise
   */
  public boolean addParticipantToCluster(ParticipantConfig participant) {
    if (participant == null) {
      LOG.error("Participant not initialized");
      return false;
    }
    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }

    ParticipantId participantId = participant.getId();
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) != null) {
      LOG.error("Config for participant: " + participantId + " already exists in cluster: "
          + _clusterId);
      return false;
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
    UserConfig userConfig = participant.getUserConfig();
    instanceConfig.addNamespacedConfig(userConfig);
    Set<String> tags = participant.getTags();
    for (String tag : tags) {
      instanceConfig.addTag(tag);
    }
    Set<PartitionId> disabledPartitions = participant.getDisabledPartitions();
    for (PartitionId partitionId : disabledPartitions) {
      instanceConfig.setInstanceEnabledForPartition(partitionId, false);
    }
    _accessor.createProperty(_keyBuilder.instanceConfig(participantId.stringify()), instanceConfig);
    _accessor.createProperty(_keyBuilder.messages(participantId.stringify()), null);
    return true;
  }

  /**
   * drop a participant from cluster
   * @param participantId
   * @return true if participant dropped, false if there was an error
   */
  public boolean dropParticipantFromCluster(ParticipantId participantId) {
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) == null) {
      LOG.error("Config for participant: " + participantId + " does NOT exist in cluster: "
          + _clusterId);
      return false;
    }

    if (_accessor.getProperty(_keyBuilder.instance(participantId.stringify())) == null) {
      LOG.error("Participant: " + participantId + " structure does NOT exist in cluster: "
          + _clusterId);
      return false;
    }

    // delete participant config path
    _accessor.removeProperty(_keyBuilder.instanceConfig(participantId.stringify()));

    // delete participant path
    _accessor.removeProperty(_keyBuilder.instance(participantId.stringify()));
    return true;
  }

  /**
   * Add a state model definition. Updates the existing state model definition if it already exists.
   * @param stateModelDef fully initialized state model definition
   * @return true if the model is persisted, false otherwise
   */
  public boolean addStateModelDefinitionToCluster(StateModelDefinition stateModelDef) {
    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }

    StateModelDefinitionAccessor smdAccessor = new StateModelDefinitionAccessor(_accessor);
    return smdAccessor.setStateModelDefinition(stateModelDef);
  }

  /**
   * Remove a state model definition if it exists
   * @param stateModelDefId state model definition id
   * @return true if removed, false if it did not exist
   */
  public boolean dropStateModelDefinitionFromCluster(StateModelDefId stateModelDefId) {
    return _accessor.removeProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify()));
  }
}
