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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.model.CurrentState;
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
   * create a new cluster
   */
  public void createCluster() {
    List<PropertyKey> createKeys = new ArrayList<PropertyKey>();

    createKeys.add(_keyBuilder.idealStates());
    createKeys.add(_keyBuilder.clusterConfigs());
    createKeys.add(_keyBuilder.instanceConfigs());
    createKeys.add(_keyBuilder.resourceConfigs());
    createKeys.add(_keyBuilder.instances());
    createKeys.add(_keyBuilder.liveInstances());
    createKeys.add(_keyBuilder.externalViews());
    createKeys.add(_keyBuilder.controller());

    // TODO add controller sub-dir's and state model definitions
    for (PropertyKey key : createKeys) {
      _accessor.createProperty(key, null);
    }
  }

  /**
   * drop a cluster
   */
  public void dropCluster() {
    LOG.info("Dropping cluster: " + _clusterId);
    List<String> liveInstanceNames =_accessor.getChildNames(_keyBuilder.liveInstances());
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

    // TODO remove cluster structure from zookeeper
  }

  /**
   * read entire cluster data
   * @return cluster
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

    Map<ResourceId, Resource> resourceMap = new HashMap<ResourceId, Resource>();
    for (String resourceName : idealStateMap.keySet()) {
      IdealState idealState = idealStateMap.get(resourceName);

      // TODO pass resource assignment
      ResourceId resourceId = new ResourceId(resourceName);
      resourceMap.put(resourceId, new Resource(resourceId, idealState, null));
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

    return new Cluster(_clusterId, resourceMap, participantMap, controllerMap, leaderId);
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
  public void resume() {
    _accessor.removeProperty(_keyBuilder.pause());
  }

  /**
   * add a resource to cluster
   * @param resource
   */
  public void addResourceToCluster(Resource resource) {
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

    // TODO convert rebalancerConfig to idealState
    _accessor.createProperty(_keyBuilder.idealState(resourceId.stringify()), null);
  }

  /**
   * drop a resource from cluster
   * @param resourceId
   */
  public void dropResourceFromCluster(ResourceId resourceId) {
    // TODO check existence
    _accessor.removeProperty(_keyBuilder.idealState(resourceId.stringify()));
    _accessor.removeProperty(_keyBuilder.resourceConfig(resourceId.stringify()));
  }

  /**
   * check if cluster structure is valid
   * @return true if valid or false otherwise
   */
  public boolean isClusterStructureValid() {
    // TODO impl this
    return false;
  }

  /**
   * add a participant to cluster
   * @param participant
   */
  public void addParticipantToCluster(Participant participant) {
    if (!isClusterStructureValid()) {
      throw new HelixException("Cluster: " + _clusterId + " structure is not valid");
    }

    ParticipantId participantId = participant.getId();
    if (_accessor.getProperty(_keyBuilder.instanceConfig(participantId.stringify())) != null) {
      throw new HelixException("Config for participant: " + participantId
          + " already exists in cluster: " + _clusterId);
    }

    List<PropertyKey> createKeys = new ArrayList<PropertyKey>();
    createKeys.add(_keyBuilder.instanceConfig(participantId.stringify()));
    createKeys.add(_keyBuilder.messages(participantId.stringify()));
    createKeys.add(_keyBuilder.currentStates(participantId.stringify()));
    // TODO add participant error and status-update paths

    for (PropertyKey key : createKeys) {
      _accessor.createProperty(key, null);
    }
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
