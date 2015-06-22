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
import java.util.Map;

import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ContextId;
import org.apache.helix.api.id.ControllerId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.SpectatorId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.context.ControllerContext;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Transition;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * Represent a logical helix cluster
 */
public class Cluster {

  /**
   * map of resource-id to resource
   */
  private final Map<ResourceId, Resource> _resourceMap;

  /**
   * map of participant-id to participant
   */
  private final Map<ParticipantId, Participant> _participantMap;

  /**
   * map of participant-id to live participant
   */
  private final Map<ParticipantId, Participant> _liveParticipantMap;

  /**
   * map of controller-id to controller
   */
  private final Map<ControllerId, Controller> _controllerMap;

  /**
   * map of spectator-id to spectator
   */
  private final Map<SpectatorId, Spectator> _spectatorMap;

  private final Map<ContextId, ControllerContext> _contextMap;

  private final ControllerId _leaderId;

  private final ClusterConfig _config;

  private final ClusterDataCache _cache;

  /**
   * construct a cluster
   * @param id
   * @param resourceMap
   * @param participantMap
   * @param controllerMap
   * @param leaderId
   * @param constraintMap
   * @param stateModelMap
   * @param contextMap
   * @param userConfig
   * @param isPaused
   * @param autoJoinAllowed
   */
  public Cluster(ClusterId id, Map<ResourceId, Resource> resourceMap,
      Map<ParticipantId, Participant> participantMap, Map<ControllerId, Controller> controllerMap,
      ControllerId leaderId, Map<ConstraintType, ClusterConstraints> constraintMap,
      Map<StateModelDefId, StateModelDefinition> stateModelMap,
      Map<ContextId, ControllerContext> contextMap, UserConfig userConfig, boolean isPaused,
      boolean autoJoinAllowed, ClusterDataCache cache) {

    // build the config
    // Guava's transform and "copy" functions really return views so the maps all reflect each other
    Map<ResourceId, ResourceConfig> resourceConfigMap =
        Maps.transformValues(resourceMap, new Function<Resource, ResourceConfig>() {
          @Override
          public ResourceConfig apply(Resource resource) {
            return resource.getConfig();
          }
        });
    Map<ParticipantId, ParticipantConfig> participantConfigMap =
        Maps.transformValues(participantMap, new Function<Participant, ParticipantConfig>() {
          @Override
          public ParticipantConfig apply(Participant participant) {
            return participant.getConfig();
          }
        });
    _config =
        new ClusterConfig.Builder(id).addResources(resourceConfigMap.values())
            .addParticipants(participantConfigMap.values()).addConstraints(constraintMap.values())
            .addStateModelDefinitions(stateModelMap.values()).pausedStatus(isPaused)
            .userConfig(userConfig).autoJoin(autoJoinAllowed).build();

    _resourceMap = ImmutableMap.copyOf(resourceMap);

    _participantMap = ImmutableMap.copyOf(participantMap);

    // Build the subset of participants that is live
    ImmutableMap.Builder<ParticipantId, Participant> liveParticipantBuilder =
        new ImmutableMap.Builder<ParticipantId, Participant>();
    for (Participant participant : participantMap.values()) {
      if (participant.isAlive()) {
        liveParticipantBuilder.put(participant.getId(), participant);
      }
    }
    _liveParticipantMap = liveParticipantBuilder.build();

    _leaderId = leaderId;

    _contextMap = ImmutableMap.copyOf(contextMap);

    _cache = cache;

    // TODO impl this when we persist controllers and spectators on zookeeper
    _controllerMap = ImmutableMap.copyOf(controllerMap);
    _spectatorMap = Collections.emptyMap();
  }

  /**
   * Get cluster id
   * @return cluster id
   */
  public ClusterId getId() {
    return _config.getId();
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
   * Get live participants of the cluster
   * @return a map of participant id to participant, or empty map if none is live
   */
  public Map<ParticipantId, Participant> getLiveParticipantMap() {
    return _liveParticipantMap;
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

  /**
   * Get all the constraints on the cluster
   * @return map of constraint type to constraints
   */
  public Map<ConstraintType, ClusterConstraints> getConstraintMap() {
    return _config.getConstraintMap();
  }

  /**
   * Get all the state model definitions on the cluster
   * @return map of state model definition id to state model definition
   */
  public Map<StateModelDefId, StateModelDefinition> getStateModelMap() {
    return _config.getStateModelMap();
  }

  /**
   * Get all the controller context currently on the cluster
   * @return map of context id to controller context objects
   */
  public Map<ContextId, ControllerContext> getContextMap() {
    return _contextMap;
  }

  /**
   * Get user-specified configuration properties of this cluster
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _config.getUserConfig();
  }

  /**
   * Get a cluster constraint
   * @param type the type of constrant to query
   * @return cluster constraints, or null if none
   */
  public ClusterConstraints getConstraint(ConstraintType type) {
    return _config.getConstraintMap().get(type);
  }

  /**
   * Get the limit of simultaneous execution of a transition
   * @param scope the scope under which the transition is constrained
   * @param stateModelDefId the state model of which the transition is a part
   * @param transition the constrained transition
   * @return the limit, or Integer.MAX_VALUE if there is no limit
   */
  public int getTransitionConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
      Transition transition) {
    return _config.getTransitionConstraint(scope, stateModelDefId, transition);
  }

  /**
   * Check the paused status of the cluster
   * @return true if paused, false otherwise
   */
  public boolean isPaused() {
    return _config.isPaused();
  }

  /**
   * Check if the cluster supports participants automatically joining
   * @return true if allowed, false if disallowed
   */
  public boolean autoJoinAllowed() {
    return _config.autoJoinAllowed();
  }

  /**
   * Get the ClusterConfig specifying this cluster
   * @return ClusterConfig
   */
  public ClusterConfig getConfig() {
    return _config;
  }

  /**
   * Get a ClusterDataCache object which is a flattened version of the physical properties read to
   * build this object.
   * @return ClusterDataCache
   */
  public ClusterDataCache getCache() {
    return _cache;
  }
}
