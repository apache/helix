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

import org.I0Itec.zkclient.DataUpdater;
import org.apache.helix.AccessOption;
import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.alerts.AlertsHolder;
import org.apache.helix.alerts.StatsHolder;
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
import org.apache.helix.api.id.ConstraintId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SessionId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.rebalancer.context.RebalancerConfig;
import org.apache.helix.controller.rebalancer.context.RebalancerContext;
import org.apache.helix.model.Alerts;
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
import org.apache.helix.model.PersistentStats;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;
import org.testng.internal.annotations.Sets;

import com.google.common.collect.Maps;

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
    ClusterConfiguration clusterConfig = ClusterConfiguration.from(cluster.getUserConfig());
    if (cluster.autoJoinAllowed()) {
      clusterConfig.setAutoJoinAllowed(cluster.autoJoinAllowed());
    }
    if (cluster.getStats() != null && !cluster.getStats().getMapFields().isEmpty()) {
      _accessor.createProperty(_keyBuilder.persistantStat(), cluster.getStats());
    }
    _accessor.createProperty(_keyBuilder.clusterConfig(), clusterConfig);
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
    if (config.getStats() == null || config.getStats().getMapFields().isEmpty()) {
      _accessor.removeProperty(_keyBuilder.persistantStat());
    } else {
      _accessor.setProperty(_keyBuilder.persistantStat(), config.getStats());
    }
    if (config.getAlerts() == null || config.getAlerts().getMapFields().isEmpty()) {
      _accessor.removeProperty(_keyBuilder.alerts());
    } else {
      _accessor.setProperty(_keyBuilder.alerts(), config.getAlerts());
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
    LiveInstance leader = _accessor.getProperty(_keyBuilder.controllerLeader());

    /**
     * map of constraint-type to constraints
     */
    Map<String, ClusterConstraints> constraintMap =
        _accessor.getChildValuesMap(_keyBuilder.constraints());

    // read all the resources
    Map<ResourceId, Resource> resourceMap = readResources();

    // read all the participants
    Map<ParticipantId, Participant> participantMap = readParticipants();

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
    boolean autoJoinAllowed = false;
    UserConfig userConfig;
    if (clusterUserConfig != null) {
      userConfig = UserConfig.from(clusterUserConfig);
      autoJoinAllowed = clusterUserConfig.autoJoinAllowed();
    } else {
      userConfig = new UserConfig(Scope.cluster(_clusterId));
    }

    // read the state model definitions
    Map<StateModelDefId, StateModelDefinition> stateModelMap = readStateModelDefinitions();

    // read the stats
    PersistentStats stats = _accessor.getProperty(_keyBuilder.persistantStat());

    // read the alerts
    Alerts alerts = _accessor.getProperty(_keyBuilder.alerts());

    // create the cluster snapshot object
    return new Cluster(_clusterId, resourceMap, participantMap, controllerMap, leaderId,
        clusterConstraintMap, stateModelMap, stats, alerts, userConfig, isPaused, autoJoinAllowed);
  }

  /**
   * Get all the state model definitions for this cluster
   * @return map of state model def id to state model definition
   */
  public Map<StateModelDefId, StateModelDefinition> readStateModelDefinitions() {
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = Maps.newHashMap();
    List<StateModelDefinition> stateModelList =
        _accessor.getChildValues(_keyBuilder.stateModelDefs());
    for (StateModelDefinition stateModelDef : stateModelList) {
      stateModelDefs.put(stateModelDef.getStateModelDefId(), stateModelDef);
    }
    return stateModelDefs;
  }

  /**
   * Read all resource in the cluster
   * @return map of resource id to resource
   */
  public Map<ResourceId, Resource> readResources() {
    /**
     * map of resource-id to ideal-state
     */
    Map<String, IdealState> idealStateMap = _accessor.getChildValuesMap(_keyBuilder.idealStates());

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
    Set<String> allResources = Sets.newHashSet();
    allResources.addAll(idealStateMap.keySet());
    allResources.addAll(resourceConfigMap.keySet());
    Map<ResourceId, Resource> resourceMap = Maps.newHashMap();
    for (String resourceName : allResources) {
      ResourceId resourceId = ResourceId.from(resourceName);
      resourceMap.put(resourceId, ResourceAccessor.createResource(resourceId,
          resourceConfigMap.get(resourceName), idealStateMap.get(resourceName),
          externalViewMap.get(resourceName), resourceAssignmentMap.get(resourceName)));
    }

    return resourceMap;
  }

  /**
   * Read all participants in the cluster
   * @return map of participant id to participant
   */
  public Map<ParticipantId, Participant> readParticipants() {
    /**
     * map of instance-id to instance-config
     */
    Map<String, InstanceConfig> instanceConfigMap =
        _accessor.getChildValuesMap(_keyBuilder.instanceConfigs());

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

    // read all the participants
    Map<ParticipantId, Participant> participantMap = Maps.newHashMap();
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

    return participantMap;
  }

  /**
   * Get cluster constraints of a given type
   * @param type ConstraintType value
   * @return ClusterConstraints, or null if none present
   */
  public ClusterConstraints readConstraints(ConstraintType type) {
    return _accessor.getProperty(_keyBuilder.constraint(type.toString()));
  }

  /**
   * Remove a constraint from the cluster
   * @param type the constraint type
   * @param constraintId the constraint id
   * @return true if removed, false otherwise
   */
  public boolean removeConstraint(ConstraintType type, ConstraintId constraintId) {
    ClusterConstraints constraints = _accessor.getProperty(_keyBuilder.constraint(type.toString()));
    if (constraints == null || constraints.getConstraintItem(constraintId) == null) {
      LOG.error("Constraint with id " + constraintId + " not present");
      return false;
    }
    constraints.removeConstraintItem(constraintId);
    return _accessor.setProperty(_keyBuilder.constraint(type.toString()), constraints);
  }

  /**
   * Read the user config of the cluster
   * @return UserConfig, or null
   */
  public UserConfig readUserConfig() {
    ClusterConfiguration clusterConfig = _accessor.getProperty(_keyBuilder.clusterConfig());
    return clusterConfig != null ? UserConfig.from(clusterConfig) : null;
  }

  /**
   * Set the user config of the cluster, overwriting existing user configs
   * @param userConfig the new user config
   * @return true if the user config was set, false otherwise
   */
  public boolean setUserConfig(UserConfig userConfig) {
    ClusterConfig.Delta delta = new ClusterConfig.Delta(_clusterId).setUserConfig(userConfig);
    return updateCluster(delta) != null;
  }

  /**
   * Clear any user-specified configuration from the cluster
   * @return true if the config was cleared, false otherwise
   */
  public boolean dropUserConfig() {
    return setUserConfig(new UserConfig(Scope.cluster(_clusterId)));
  }

  /**
   * Get the stats persisted on this cluster
   * @return PersistentStats, or null if none persisted
   */
  public PersistentStats readStats() {
    return _accessor.getProperty(_keyBuilder.persistantStat());
  }

  /**
   * Add a statistic specification to the cluster. Existing stat specifications will not be
   * overwritten
   * @param statName string representing a stat specification
   * @return true if the stat spec was added, false otherwise
   */
  public boolean addStat(final String statName) {
    if (!isClusterStructureValid()) {
      throw new HelixException("cluster " + _clusterId + " is not setup yet");
    }

    String persistentStatsPath = _keyBuilder.persistantStat().getPath();
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    return baseAccessor.update(persistentStatsPath, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord statsRec) {
        if (statsRec == null) {
          statsRec = new ZNRecord(PersistentStats.nodeName);
        }
        Map<String, Map<String, String>> currStatMap = statsRec.getMapFields();
        Map<String, Map<String, String>> newStatMap = StatsHolder.parseStat(statName);
        for (String newStat : newStatMap.keySet()) {
          if (!currStatMap.containsKey(newStat)) {
            currStatMap.put(newStat, newStatMap.get(newStat));
          }
        }
        statsRec.setMapFields(currStatMap);
        return statsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * Remove a statistic specification from the cluster
   * @param statName string representing a statistic specification
   * @return true if stats removed, false otherwise
   */
  public boolean dropStat(final String statName) {
    if (!isClusterStructureValid()) {
      throw new HelixException("cluster " + _clusterId + " is not setup yet");
    }

    String persistentStatsPath = _keyBuilder.persistantStat().getPath();
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    return baseAccessor.update(persistentStatsPath, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord statsRec) {
        if (statsRec == null) {
          throw new HelixException("No stats record in ZK, nothing to drop");
        }
        Map<String, Map<String, String>> currStatMap = statsRec.getMapFields();
        Map<String, Map<String, String>> newStatMap = StatsHolder.parseStat(statName);
        // delete each stat from stat map
        for (String newStat : newStatMap.keySet()) {
          if (currStatMap.containsKey(newStat)) {
            currStatMap.remove(newStat);
          }
        }
        statsRec.setMapFields(currStatMap);
        return statsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * Add an alert specification to the cluster
   * @param alertName string representing the alert spec
   * @return true if added, false otherwise
   */
  public boolean addAlert(final String alertName) {
    if (!isClusterStructureValid()) {
      throw new HelixException("cluster " + _clusterId + " is not setup yet");
    }

    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    String alertsPath = _keyBuilder.alerts().getPath();
    return baseAccessor.update(alertsPath, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord alertsRec) {
        if (alertsRec == null) {
          alertsRec = new ZNRecord(Alerts.nodeName);
        }
        Map<String, Map<String, String>> currAlertMap = alertsRec.getMapFields();
        StringBuilder newStatName = new StringBuilder();
        Map<String, String> newAlertMap = new HashMap<String, String>();

        // use AlertsHolder to get map of new stats and map for this alert
        AlertsHolder.parseAlert(alertName, newStatName, newAlertMap);

        // add stat
        addStat(newStatName.toString());

        // add alert
        currAlertMap.put(alertName, newAlertMap);
        alertsRec.setMapFields(currAlertMap);
        return alertsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * Remove an alert specification from the cluster
   * @param alertName string representing an alert specification
   * @return true if removed, false otherwise
   */
  public boolean dropAlert(final String alertName) {
    if (!isClusterStructureValid()) {
      throw new HelixException("cluster " + _clusterId + " is not setup yet");
    }

    String alertsPath = _keyBuilder.alerts().getPath();
    BaseDataAccessor<ZNRecord> baseAccessor = _accessor.getBaseDataAccessor();
    return baseAccessor.update(alertsPath, new DataUpdater<ZNRecord>() {
      @Override
      public ZNRecord update(ZNRecord alertsRec) {
        if (alertsRec == null) {
          throw new HelixException("No alerts record persisted, nothing to drop");
        }
        Map<String, Map<String, String>> currAlertMap = alertsRec.getMapFields();
        currAlertMap.remove(alertName);
        alertsRec.setMapFields(currAlertMap);
        return alertsRec;
      }
    }, AccessOption.PERSISTENT);
  }

  /**
   * Add user configuration to the existing cluster user configuration. Overwrites properties with
   * the same key
   * @param userConfig the user config key-value pairs to add
   * @return true if the user config was updated, false otherwise
   */
  public boolean updateUserConfig(UserConfig userConfig) {
    ClusterConfiguration clusterConfig = new ClusterConfiguration(_clusterId);
    clusterConfig.addNamespacedConfig(userConfig);
    return _accessor.updateProperty(_keyBuilder.clusterConfig(), clusterConfig);
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
    return isClusterStructureValid(_clusterId, _accessor.getBaseDataAccessor());
  }

  /**
   * check if cluster structure is valid
   * @param clusterId the cluster to check
   * @param baseAccessor a base data accessor
   * @return true if valid or false otherwise
   */
  private static boolean isClusterStructureValid(ClusterId clusterId,
      BaseDataAccessor<?> baseAccessor) {
    List<String> paths = getRequiredPaths(clusterId);
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
    List<String> paths = getRequiredPaths(_clusterId);
    for (String path : paths) {
      boolean status = baseAccessor.create(path, null, AccessOption.PERSISTENT);
      if (!status && LOG.isDebugEnabled()) {
        LOG.debug(path + " already exists");
      }
    }
  }

  /**
   * Get all property paths that must be set for a cluster structure to be valid
   * @param the cluster that the paths will be relative to
   * @return list of paths as strings
   */
  private static List<String> getRequiredPaths(ClusterId clusterId) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterId.stringify());
    List<String> paths = new ArrayList<String>();
    paths.add(keyBuilder.cluster().getPath());
    paths.add(keyBuilder.clusterConfigs().getPath());
    paths.add(keyBuilder.instanceConfigs().getPath());
    paths.add(keyBuilder.propertyStore().getPath());
    paths.add(keyBuilder.liveInstances().getPath());
    paths.add(keyBuilder.instances().getPath());
    paths.add(keyBuilder.externalViews().getPath());
    paths.add(keyBuilder.controller().getPath());
    paths.add(keyBuilder.stateModelDefs().getPath());
    paths.add(keyBuilder.controllerMessages().getPath());
    paths.add(keyBuilder.controllerTaskErrors().getPath());
    paths.add(keyBuilder.controllerTaskStatuses().getPath());
    paths.add(keyBuilder.controllerLeaderHistory().getPath());
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
    ParticipantAccessor accessor = new ParticipantAccessor(_accessor);
    return accessor.dropParticipant(participantId);
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

    return _accessor
        .createProperty(_keyBuilder.stateModelDef(stateModelDef.getId()), stateModelDef);
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
