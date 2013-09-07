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

import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;

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

  private final ControllerId _leaderId;

  private final ClusterConfig _config;

  /**
   * construct a cluster
   * @param id
   * @param resourceMap
   * @param participantMap
   * @param controllerMap
   * @param leaderId
   */
  public Cluster(ClusterId id, Map<ResourceId, Resource> resourceMap,
      Map<ParticipantId, Participant> participantMap, Map<ControllerId, Controller> controllerMap,
      ControllerId leaderId, Map<ConstraintType, ClusterConstraints> constraintMap, boolean isPaused) {

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
        new ClusterConfig(id, resourceConfigMap, participantConfigMap, constraintMap, isPaused);

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
   * Get a cluster constraint
   * @param type the type of constrant to query
   * @return cluster constraints, or null if none
   */
  public ClusterConstraints getConstraint(ConstraintType type) {
    return _config.getConstraintMap().get(type);
  }

  /**
   * Check the pasued status of the cluster
   * @return true if paused, false otherwise
   */
  public boolean isPaused() {
    return _config.isPaused();
  }
}
