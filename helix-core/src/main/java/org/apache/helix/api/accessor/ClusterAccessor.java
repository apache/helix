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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import org.apache.helix.api.config.ContainerConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ContextId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.context.ControllerContext;
import org.apache.helix.controller.context.ControllerContextHolder;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.helix.controller.stages.ClusterDataCache;
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
import org.apache.helix.model.ProvisionerConfigHolder;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

public class ClusterAccessor {
  private static Logger LOG = Logger.getLogger(ClusterAccessor.class);

  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final ClusterId _clusterId;

  private final ClusterDataCache _cache;

  /**
   * Instantiate a cluster accessor
   * @param clusterId the cluster to access
   * @param accessor HelixDataAccessor for the physical store
   */
  public ClusterAccessor(ClusterId clusterId, HelixDataAccessor accessor) {
    this(clusterId, accessor, new ClusterDataCache());
  }

  /**
   * Instantiate a cluster accessor
   * @param clusterId the cluster to access
   * @param accessor HelixDataAccessor for the physical store
   */
  public ClusterAccessor(ClusterId clusterId, HelixDataAccessor accessor, ClusterDataCache cache) {
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
    _clusterId = clusterId;
    _cache = cache;
  }

  /**
   * create a new cluster, fail if it already exists
   * @return true if created, false if creation failed
   */
  public boolean createCluster(ClusterConfig cluster) {
    ClusterConfiguration configuration = _accessor.getProperty(_keyBuilder.clusterConfig());
    if (configuration != null && isClusterStructureValid()) {
      LOG.error("Cluster already created. Aborting.");
      return false;
    }
    clearClusterStructure();
    initClusterStructure();
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = cluster.getStateModelMap();
    for (StateModelDefinition stateModelDef : stateModelDefs.values()) {
      addStateModelDefinition(stateModelDef);
    }
    Map<ResourceId, ResourceConfig> resources = cluster.getResourceMap();
    for (ResourceConfig resource : resources.values()) {
      addResource(resource);
    }
    Map<ParticipantId, ParticipantConfig> participants = cluster.getParticipantMap();
    for (ParticipantConfig participant : participants.values()) {
      addParticipant(participant);
    }
    _accessor.createProperty(_keyBuilder.constraints(), null);
    for (ClusterConstraints constraints : cluster.getConstraintMap().values()) {
      _accessor.setProperty(_keyBuilder.constraint(constraints.getType().toString()), constraints);
    }
    ClusterConfiguration clusterConfig =
        ClusterConfiguration.from(_clusterId, cluster.getUserConfig());
    if (cluster.autoJoinAllowed()) {
      clusterConfig.setAutoJoinAllowed(cluster.autoJoinAllowed());
    }
    if (cluster.isPaused()) {
      _accessor.createProperty(_keyBuilder.pause(), new PauseSignal("pause"));
    }
    _accessor.setProperty(_keyBuilder.clusterConfig(), clusterConfig);

    return true;
  }

  /**
   * Update the cluster configuration
   * @param clusterDelta change to the cluster configuration
   * @return updated ClusterConfig, or null if there was an error
   */
  public ClusterConfig updateCluster(ClusterConfig.Delta clusterDelta) {
    clusterDelta.merge(_accessor);
    Cluster cluster = readCluster();
    return (cluster != null) ? cluster.getConfig() : null;
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
   * @return cluster snapshot or null
   */
  public Cluster readCluster() {
    if (!isClusterStructureValid()) {
      LOG.error("Cluster is not fully set up");
      return null;
    }

    // refresh the cache
    _cache.refresh(_accessor);

    LiveInstance leader = _cache.getLeader();

    /**
     * map of constraint-type to constraints
     */
    Map<String, ClusterConstraints> constraintMap = _cache.getConstraintMap();

    // read all the resources
    Map<ResourceId, Resource> resourceMap = readResources(true);

    // read all the participants
    Map<ParticipantId, Participant> participantMap = readParticipants(true);

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
    PauseSignal pauseSignal = _cache.getPauseSignal();
    boolean isPaused = pauseSignal != null;

    ClusterConfiguration clusterConfig = _cache.getClusterConfig();
    boolean autoJoinAllowed = false;
    UserConfig userConfig;
    if (clusterConfig != null) {
      userConfig = clusterConfig.getUserConfig();
      autoJoinAllowed = clusterConfig.autoJoinAllowed();
    } else {
      userConfig = new UserConfig(Scope.cluster(_clusterId));
    }

    // read the state model definitions
    Map<StateModelDefId, StateModelDefinition> stateModelMap = readStateModelDefinitions(true);

    // read controller context
    Map<ContextId, ControllerContext> contextMap = readControllerContext(true);

    // create the cluster snapshot object
    return new Cluster(_clusterId, resourceMap, participantMap, controllerMap, leaderId,
        clusterConstraintMap, stateModelMap, contextMap, userConfig, isPaused, autoJoinAllowed,
        _cache);
  }

  /**
   * Get all the state model definitions for this cluster
   * @param useCache Use the ClusterDataCache associated with this class rather than reading again
   * @return map of state model def id to state model definition
   */
  private Map<StateModelDefId, StateModelDefinition> readStateModelDefinitions(boolean useCache) {
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = Maps.newHashMap();
    Collection<StateModelDefinition> stateModelList;
    if (useCache) {
      stateModelList = _cache.getStateModelDefMap().values();
    } else {
      stateModelList = _accessor.getChildValues(_keyBuilder.stateModelDefs());
    }
    for (StateModelDefinition stateModelDef : stateModelList) {
      stateModelDefs.put(stateModelDef.getStateModelDefId(), stateModelDef);
    }
    return stateModelDefs;
  }

  /**
   * Read all resources in the cluster
   * @param useCache Use the ClusterDataCache associated with this class rather than reading again
   * @return map of resource id to resource
   */
  private Map<ResourceId, Resource> readResources(boolean useCache) {
    if (!useCache && !isClusterStructureValid()) {
      LOG.error("Cluster is not fully set up yet!");
      return Collections.emptyMap();
    }

    Map<String, IdealState> idealStateMap;
    Map<String, ResourceConfiguration> resourceConfigMap;
    Map<String, ExternalView> externalViewMap;
    Map<String, ResourceAssignment> resourceAssignmentMap;
    if (useCache) {
      idealStateMap = _cache.getIdealStates();
      resourceConfigMap = _cache.getResourceConfigs();
    } else {
      idealStateMap = _accessor.getChildValuesMap(_keyBuilder.idealStates());
      resourceConfigMap = _accessor.getChildValuesMap(_keyBuilder.resourceConfigs());
    }

    // now read external view and resource assignments if needed
    if (!useCache) {
      externalViewMap = _accessor.getChildValuesMap(_keyBuilder.externalViews());
      resourceAssignmentMap = _accessor.getChildValuesMap(_keyBuilder.resourceAssignments());
      _cache.setAssignmentWritePolicy(true);
    } else {
      externalViewMap = Maps.newHashMap();
      resourceAssignmentMap = Maps.newHashMap();
      _cache.setAssignmentWritePolicy(false);
    }

    // populate all the resources
    Map<ResourceId, Resource> resourceMap = Maps.newHashMap();
    for (String resourceName : idealStateMap.keySet()) {
      ResourceId resourceId = ResourceId.from(resourceName);
      resourceMap.put(
          resourceId,
          createResource(resourceId, resourceConfigMap.get(resourceName),
              idealStateMap.get(resourceName), externalViewMap.get(resourceName),
              resourceAssignmentMap.get(resourceName)));
    }

    return resourceMap;
  }

  /**
   * Read all participants in the cluster
   * @param useCache Use the ClusterDataCache associated with this class rather than reading again
   * @return map of participant id to participant, or empty map
   */
  private Map<ParticipantId, Participant> readParticipants(boolean useCache) {
    if (!useCache && !isClusterStructureValid()) {
      LOG.error("Cluster is not fully set up yet!");
      return Collections.emptyMap();
    }
    Map<String, InstanceConfig> instanceConfigMap;
    Map<String, LiveInstance> liveInstanceMap;
    if (useCache) {
      instanceConfigMap = _cache.getInstanceConfigMap();
      liveInstanceMap = _cache.getLiveInstances();
    } else {
      instanceConfigMap = _accessor.getChildValuesMap(_keyBuilder.instanceConfigs());
      liveInstanceMap = _accessor.getChildValuesMap(_keyBuilder.liveInstances());
    }

    /**
     * map of participant-id to map of message-id to message
     */
    Map<String, Map<String, Message>> messageMap = Maps.newHashMap();
    for (String participantName : liveInstanceMap.keySet()) {
      Map<String, Message> instanceMsgMap;
      if (useCache) {
        instanceMsgMap = _cache.getMessages(participantName);
      } else {
        instanceMsgMap = _accessor.getChildValuesMap(_keyBuilder.messages(participantName));
      }
      messageMap.put(participantName, instanceMsgMap);
    }

    /**
     * map of participant-id to map of resource-id to current-state
     */
    Map<String, Map<String, CurrentState>> currentStateMap = Maps.newHashMap();
    for (String participantName : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(participantName);
      SessionId sessionId = liveInstance.getTypedSessionId();
      Map<String, CurrentState> instanceCurStateMap;
      if (useCache) {
        instanceCurStateMap = _cache.getCurrentState(participantName, sessionId.stringify());
      } else {
        instanceCurStateMap =
            _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
                sessionId.stringify()));
      }
      currentStateMap.put(participantName, instanceCurStateMap);
    }

    // read all the participants
    Map<ParticipantId, Participant> participantMap = Maps.newHashMap();
    for (String participantName : instanceConfigMap.keySet()) {
      InstanceConfig instanceConfig = instanceConfigMap.get(participantName);
      UserConfig userConfig = instanceConfig.getUserConfig();
      LiveInstance liveInstance = liveInstanceMap.get(participantName);
      Map<String, Message> instanceMsgMap = messageMap.get(participantName);

      ParticipantId participantId = ParticipantId.from(participantName);

      participantMap.put(
          participantId,
          createParticipant(participantId, instanceConfig, userConfig, liveInstance,
              instanceMsgMap, currentStateMap.get(participantName)));
    }

    return participantMap;
  }

  /**
   * Read the persisted controller contexts
   * @param useCache Use the ClusterDataCache associated with this class rather than reading again
   * @return map of context id to controller context
   */
  private Map<ContextId, ControllerContext> readControllerContext(boolean useCache) {
    Map<String, ControllerContextHolder> contextHolders;
    if (useCache) {
      contextHolders = _cache.getContextMap();
    } else {
      contextHolders = _accessor.getChildValuesMap(_keyBuilder.controllerContexts());
    }
    Map<ContextId, ControllerContext> contexts = Maps.newHashMap();
    for (String contextName : contextHolders.keySet()) {
      contexts.put(ContextId.from(contextName), contextHolders.get(contextName).getContext());
    }
    return contexts;
  }

  /**
   * add a resource to cluster
   * @param resource
   * @return true if resource added, false if there was an error
   */
  public boolean addResource(ResourceConfig resource) {
    if (resource == null || resource.getIdealState() == null) {
      LOG.error("Resource not fully defined with an ideal state");
      return false;
    }

    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }
    IdealState idealState = resource.getIdealState();
    StateModelDefId stateModelDefId = idealState.getStateModelDefId();
    if (_accessor.getProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify())) == null) {
      LOG.error("State model: " + stateModelDefId + " not found in cluster: " + _clusterId);
      return false;
    }

    ResourceId resourceId = resource.getId();
    if (_accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify())) != null) {
      LOG.error("Skip adding resource: " + resourceId
          + ", because resource ideal state already exists in cluster: " + _clusterId);
      return false;
    }
    if (_accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify())) != null) {
      LOG.error("Skip adding resource: " + resourceId
          + ", because resource config already exists in cluster: " + _clusterId);
      return false;
    }

    // Persist the ideal state
    _accessor.setProperty(_keyBuilder.idealStates(resourceId.stringify()), idealState);

    // Add resource user config
    boolean persistConfig = false;
    ResourceConfiguration configuration = new ResourceConfiguration(resourceId);
    if (resource.getUserConfig() != null) {
      configuration.addNamespacedConfig(resource.getUserConfig());
      persistConfig = true;
    }
    RebalancerConfig rebalancerConfig = resource.getRebalancerConfig();
    if (rebalancerConfig != null) {
      // only persist if this is not easily convertible to an ideal state
      configuration.addNamespacedConfig(new RebalancerConfigHolder(rebalancerConfig)
          .toNamespacedConfig());
      persistConfig = true;
    }
    ProvisionerConfig provisionerConfig = resource.getProvisionerConfig();
    if (provisionerConfig != null) {
      configuration.addNamespacedConfig(new ProvisionerConfigHolder(provisionerConfig)
          .toNamespacedConfig());
      persistConfig = true;
    }
    if (persistConfig) {
      _accessor.setProperty(_keyBuilder.resourceConfig(resourceId.stringify()), configuration);
    }
    return true;
  }

  /**
   * drop a resource from cluster
   * @param resourceId
   * @return true if removal succeeded, false otherwise
   */
  public boolean dropResource(ResourceId resourceId) {
    if (_accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify())) == null) {
      LOG.error("Skip removing resource: " + resourceId
          + ", because resource ideal state already removed from cluster: " + _clusterId);
      return false;
    }
    _accessor.removeProperty(_keyBuilder.idealStates(resourceId.stringify()));
    _accessor.removeProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    return true;
  }

  /**
   * check if cluster structure is valid
   * @return true if valid or false otherwise
   */
  protected boolean isClusterStructureValid() {
    List<String> paths = HelixUtil.getRequiredPathsForCluster(_clusterId.toString());
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    if (baseAccessor != null) {
      boolean[] existsResults = baseAccessor.exists(paths, 0);
      int ind = 0;
      for (boolean exists : existsResults) {
        if (!exists) {
          LOG.warn("Path does not exist:" + paths.get(ind));
          return false;
        }
        ind = ind + 1;
      }
    }
    return true;
  }

  /**
   * Create empty persistent properties to ensure that there is a valid cluster structure
   */
  private void initClusterStructure() {
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    List<String> paths = HelixUtil.getRequiredPathsForCluster(_clusterId.toString());
    for (String path : paths) {
      boolean status = baseAccessor.create(path, null, AccessOption.PERSISTENT);
      if (!status && LOG.isDebugEnabled()) {
        LOG.debug(path + " already exists");
      }
    }
  }

  /**
   * Remove all but the top level cluster node; intended for reconstructing the cluster
   */
  private void clearClusterStructure() {
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    List<String> paths = HelixUtil.getRequiredPathsForCluster(_clusterId.toString());
    baseAccessor.remove(paths, 0);
  }

  /**
   * add a participant to cluster
   * @param participant
   * @return true if participant added, false otherwise
   */
  public boolean addParticipant(ParticipantConfig participant) {
    if (participant == null) {
      LOG.error("Participant not initialized");
      return false;
    }
    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }

    ParticipantId participantId = participant.getId();
    InstanceConfig existConfig =
        _accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify()));
    if (existConfig != null && isParticipantStructureValid(participantId)) {
      LOG.error("Config for participant: " + participantId + " already exists in cluster: "
          + _clusterId);
      return false;
    }

    // clear and rebuild the participant structure
    clearParticipantStructure(participantId);
    initParticipantStructure(participantId);

    // add the config
    _accessor.setProperty(_keyBuilder.instanceConfig(participantId.stringify()),
        participant.getInstanceConfig());
    return true;
  }

  /**
   * drop a participant from cluster
   * @param participantId
   * @return true if participant dropped, false if there was an error
   */
  public boolean dropParticipant(ParticipantId participantId) {
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) == null) {
      LOG.error("Config for participant: " + participantId + " does NOT exist in cluster");
    }

    if (_accessor.getProperty(_keyBuilder.instance(participantId.stringify())) == null) {
      LOG.error("Participant: " + participantId + " structure does NOT exist in cluster");
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
  public boolean addStateModelDefinition(StateModelDefinition stateModelDef) {
    if (!isClusterStructureValid()) {
      LOG.error("Cluster: " + _clusterId + " structure is not valid");
      return false;
    }

    return _accessor
        .createProperty(_keyBuilder.stateModelDef(stateModelDef.getId()), stateModelDef);
  }

  /**
   * Remove a state model definition if it exists
   * @param stateModelDefId state model definition id
   * @return true if removed, false if it did not exist
   */
  public boolean dropStateModelDefinition(StateModelDefId stateModelDefId) {
    return _accessor.removeProperty(_keyBuilder.stateModelDef(stateModelDefId.stringify()));
  }

  /**
   * Read the leader controller if it is live
   * @return Controller snapshot, or null
   */
  public Controller readLeader() {
    LiveInstance leader = _accessor.getProperty(_keyBuilder.controllerLeader());
    if (leader != null) {
      ControllerId leaderId = ControllerId.from(leader.getId());
      return new Controller(leaderId, leader, true);
    }
    return null;
  }

  /**
   * Update a participant configuration
   * @param participantId the participant to update
   * @param participantDelta changes to the participant
   * @return ParticipantConfig, or null if participant is not persisted
   */
  public ParticipantConfig updateParticipant(ParticipantId participantId,
      ParticipantConfig.Delta participantDelta) {
    participantDelta.merge(_accessor);
    Participant participant = readParticipant(participantId);
    return (participant != null) ? participant.getConfig() : null;
  }

  /**
   * create a participant based on physical model
   * @param participantId
   * @param instanceConfig
   * @param userConfig
   * @param liveInstance
   * @param instanceMsgMap map of message-id to message
   * @param instanceCurStateMap map of resource-id to current-state
   * @return participant
   */
  private static Participant createParticipant(ParticipantId participantId,
      InstanceConfig instanceConfig, UserConfig userConfig, LiveInstance liveInstance,
      Map<String, Message> instanceMsgMap, Map<String, CurrentState> instanceCurStateMap) {
    Map<MessageId, Message> msgMap = new HashMap<MessageId, Message>();
    if (instanceMsgMap != null) {
      for (String msgId : instanceMsgMap.keySet()) {
        Message message = instanceMsgMap.get(msgId);
        msgMap.put(MessageId.from(msgId), message);
      }
    }

    Map<ResourceId, CurrentState> curStateMap = new HashMap<ResourceId, CurrentState>();
    if (instanceCurStateMap != null) {

      for (String resourceName : instanceCurStateMap.keySet()) {
        curStateMap.put(ResourceId.from(resourceName), instanceCurStateMap.get(resourceName));
      }
    }

    // set up the container config if it exists
    ContainerConfig containerConfig = null;
    ContainerSpec containerSpec = instanceConfig.getContainerSpec();
    ContainerState containerState = instanceConfig.getContainerState();
    ContainerId containerId = instanceConfig.getContainerId();
    if (containerSpec != null || containerState != null || containerId != null) {
      containerConfig = new ContainerConfig(containerId, containerSpec, containerState);
    }

    // Populate the logical class
    ParticipantConfig participantConfig = ParticipantConfig.from(instanceConfig);
    return new Participant(participantConfig, liveInstance, curStateMap, msgMap, containerConfig);
  }

  /**
   * read participant related data
   * @param participantId
   * @return participant, or null if participant not available
   */
  public Participant readParticipant(ParticipantId participantId) {
    // read physical model
    String participantName = participantId.stringify();
    InstanceConfig instanceConfig =
        _accessor.getProperty(_keyBuilder.instanceConfig(participantName));

    if (instanceConfig == null) {
      LOG.error("Participant " + participantId + " is not present on the cluster");
      return null;
    }

    UserConfig userConfig = instanceConfig.getUserConfig();
    LiveInstance liveInstance = _accessor.getProperty(_keyBuilder.liveInstance(participantName));

    Map<String, Message> instanceMsgMap = Collections.emptyMap();
    Map<String, CurrentState> instanceCurStateMap = Collections.emptyMap();
    if (liveInstance != null) {
      SessionId sessionId = liveInstance.getTypedSessionId();

      instanceMsgMap = _accessor.getChildValuesMap(_keyBuilder.messages(participantName));
      instanceCurStateMap =
          _accessor.getChildValuesMap(_keyBuilder.currentStates(participantName,
              sessionId.stringify()));
    }

    return createParticipant(participantId, instanceConfig, userConfig, liveInstance,
        instanceMsgMap, instanceCurStateMap);
  }

  /**
   * Read a single snapshot of a resource
   * @param resourceId the resource id to read
   * @return Resource or null if not present
   */
  public Resource readResource(ResourceId resourceId) {
    ResourceConfiguration config =
        _accessor.getProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
    IdealState idealState = _accessor.getProperty(_keyBuilder.idealStates(resourceId.stringify()));

    if (idealState == null) {
      LOG.error("Resource " + resourceId + " not present on the cluster");
      return null;
    }

    ExternalView externalView =
        _accessor.getProperty(_keyBuilder.externalView(resourceId.stringify()));
    ResourceAssignment resourceAssignment =
        _accessor.getProperty(_keyBuilder.resourceAssignment(resourceId.stringify()));
    return createResource(resourceId, config, idealState, externalView, resourceAssignment);
  }

  /**
   * Update a resource configuration
   * @param resourceId the resource id to update
   * @param resourceDelta changes to the resource
   * @return ResourceConfig, or null if the resource is not persisted
   */
  public ResourceConfig updateResource(ResourceId resourceId, ResourceConfig.Delta resourceDelta) {
    resourceDelta.merge(_accessor);
    Resource resource = readResource(resourceId);
    return (resource != null) ? resource.getConfig() : null;
  }

  /**
   * Create a resource snapshot instance from the physical model
   * @param resourceId the resource id
   * @param resourceConfiguration physical resource configuration
   * @param idealState ideal state of the resource
   * @param externalView external view of the resource
   * @param resourceAssignment current resource assignment
   * @return Resource
   */
  private static Resource createResource(ResourceId resourceId,
      ResourceConfiguration resourceConfiguration, IdealState idealState,
      ExternalView externalView, ResourceAssignment resourceAssignment) {
    UserConfig userConfig;
    ProvisionerConfig provisionerConfig = null;
    RebalancerConfig rebalancerConfig = null;
    if (resourceConfiguration != null) {
      userConfig = resourceConfiguration.getUserConfig();
      rebalancerConfig = resourceConfiguration.getRebalancerConfig(RebalancerConfig.class);
      provisionerConfig = resourceConfiguration.getProvisionerConfig(ProvisionerConfig.class);
    } else {
      userConfig = new UserConfig(Scope.resource(resourceId));
    }
    ResourceConfig resourceConfig =
        new ResourceConfig.Builder(resourceId).idealState(idealState)
            .rebalancerConfig(rebalancerConfig).provisionerConfig(provisionerConfig)
            .schedulerTaskConfig(Resource.schedulerTaskConfig(idealState)).userConfig(userConfig)
            .build();
    return new Resource(resourceConfig, resourceAssignment, externalView);
  }

  /**
   * Get the cluster ID this accessor is connected to
   * @return ClusterId
   */
  protected ClusterId clusterId() {
    return _clusterId;
  }

  /**
   * Get the accessor for the properties stored for this cluster
   * @return HelixDataAccessor
   */
  protected HelixDataAccessor dataAccessor() {
    return _accessor;
  }

  /**
   * Create empty persistent properties to ensure that there is a valid participant structure
   * @param participantId the identifier under which to initialize the structure
   * @return true if the participant structure exists at the end of this call, false otherwise
   */
  private boolean initParticipantStructure(ParticipantId participantId) {
    if (participantId == null) {
      LOG.error("Participant ID cannot be null when clearing the participant in cluster "
          + _clusterId + "!");
      return false;
    }
    List<String> paths =
        HelixUtil.getRequiredPathsForInstance(_clusterId.toString(), participantId.toString());
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    for (String path : paths) {
      boolean status = baseAccessor.create(path, null, AccessOption.PERSISTENT);
      if (!status) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(path + " already exists");
        }
      }
    }
    return true;
  }

  /**
   * Clear properties for the participant
   * @param participantId the participant for which to clear
   * @return true if all paths removed, false otherwise
   */
  private boolean clearParticipantStructure(ParticipantId participantId) {
    List<String> paths =
        HelixUtil.getRequiredPathsForInstance(_clusterId.toString(), participantId.toString());
    BaseDataAccessor<?> baseAccessor = _accessor.getBaseDataAccessor();
    boolean[] removeResults = baseAccessor.remove(paths, 0);
    boolean result = true;
    for (boolean removeResult : removeResults) {
      result = result && removeResult;
    }
    return result;
  }

  /**
   * check if participant structure is valid
   * @return true if valid or false otherwise
   */
  private boolean isParticipantStructureValid(ParticipantId participantId) {
    List<String> paths =
        HelixUtil.getRequiredPathsForInstance(_clusterId.toString(), participantId.toString());
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
}
