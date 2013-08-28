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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;

import com.google.common.collect.ImmutableMap;

/**
 * Represent a logical helix cluster
 */
public class Cluster {
  private final ClusterId _id;

  /**
   * map of resource-id to resource
   */
  private final Map<ResourceId, Resource> _resourceMap;

  /**
   * map of participant-id to participant
   */
  private final Map<ParticipantId, Participant> _participantMap;

  /**
   * map of controller-id to controller
   */
  private final Map<ControllerId, Controller> _controllerMap;

  /**
   * map of spectator-id to spectator
   */
  private final Map<SpectatorId, Spectator> _spectatorMap;

  private final ControllerId _leaderId;

  private final ClusterConfig _config = null;

  // TODO move construct logic to ClusterAccessor
  /**
   * Construct a cluster
   * @param id a unique id for the cluster
   * @param idealStateMap map of resource-id to ideal-state
   * @param currentStateMap map of resource-id to map of participant-id to current-state
   * @param instanceConfigMap map of participant-id to instance-config
   * @param liveInstanceMap map of participant-id to live-instance
   * @param msgMap map of participant-id to map of message-id to message
   * @param leader
   */
  public Cluster(ClusterId id, Map<String, IdealState> idealStateMap,
      Map<String, Map<String, CurrentState>> currentStateMap,
      Map<String, InstanceConfig> instanceConfigMap, Map<String, LiveInstance> liveInstanceMap,
      Map<String, Map<String, Message>> msgMap, LiveInstance leader) {
    _id = id;

    Map<ResourceId, Resource> resourceMap = new HashMap<ResourceId, Resource>();
    for (String resourceId : idealStateMap.keySet()) {
      IdealState idealState = idealStateMap.get(resourceId);
      Map<String, CurrentState> curStateMap = currentStateMap.get(resourceId);

      // TODO pass resource assignment
      resourceMap.put(new ResourceId(resourceId), new Resource(new ResourceId(resourceId),
          idealState, null));
    }
    _resourceMap = ImmutableMap.copyOf(resourceMap);

    Map<ParticipantId, Participant> participantMap = new HashMap<ParticipantId, Participant>();
    for (String participantId : instanceConfigMap.keySet()) {
      InstanceConfig instanceConfig = instanceConfigMap.get(participantId);
      LiveInstance liveInstance = liveInstanceMap.get(participantId);
      Map<String, Message> instanceMsgMap = msgMap.get(participantId);

      // TODO pass current-state map
      participantMap.put(new ParticipantId(participantId), new Participant(new ParticipantId(
          participantId), instanceConfig, liveInstance, null, instanceMsgMap));
    }
    _participantMap = ImmutableMap.copyOf(participantMap);

    Map<ControllerId, Controller> controllerMap = new HashMap<ControllerId, Controller>();
    if (leader != null) {
      _leaderId = new ControllerId(leader.getId());
      controllerMap.put(_leaderId, new Controller(_leaderId, leader, true));
    } else {
      _leaderId = null;
    }

    // TODO impl this when we persist controllers and spectators on zookeeper
    _controllerMap = ImmutableMap.copyOf(controllerMap);
    _spectatorMap = Collections.emptyMap();
  }

  /**
   * Get cluster id
   * @return cluster id
   */
  public ClusterId getId() {
    return _id;
  }

  /**
   * Get resources in the cluster
   * @return a map of resource id to resource, or empty map if none
   */
  public Map<ResourceId, Resource> getResourceMap() {
    return _resourceMap;
  }

  /**
   * Get resource given resource id
   * @param resourceId
   * @return resource or null if not exist
   */
  public Resource getResource(ResourceId resourceId) {
    return _resourceMap.get(resourceId);
  }

  /**
   * Get participants of the cluster
   * @return a map of participant id to participant, or empty map if none
   */
  public Map<ParticipantId, Participant> getParticipantMap() {
    return _participantMap;
  }

  /**
   * Get controllers of the cluster
   * @return a map of controller id to controller, or empty map if none
   */
  public Map<ControllerId, Controller> getControllerMap() {
    return _controllerMap;
  }

  /**
   * Get the leader of the cluster
   * @return the leader or null if not exist
   */
  public Controller getLeader() {
    return _controllerMap.get(_leaderId);
  }

  /**
   * Get spectators of the cluster
   * @return a map of spectator id to spectator, or empty map if none
   */
  public Map<SpectatorId, Spectator> getSpectatorMap() {
    return _spectatorMap;
  }

}
