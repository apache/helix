package org.apache.helix.api;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintType;

import com.google.common.collect.ImmutableMap;

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

/**
 * Configuration properties of a cluster
 */
public class ClusterConfig {
  private final ClusterId _id;
  private final Map<ResourceId, ResourceConfig> _resourceMap;
  private final Map<ParticipantId, ParticipantConfig> _participantMap;
  private final Map<ConstraintType, ClusterConstraints> _constraintMap;
  private final UserConfig _userConfig;
  private final boolean _isPaused;

  /**
   * Initialize a cluster configuration. Also see ClusterConfig.Builder
   * @param id cluster id
   * @param resourceMap map of resource id to resource config
   * @param participantMap map of participant id to participant config
   * @param constraintMapmap of constraint type to all constraints of that type
   * @param userConfig user-defined cluster properties
   * @param isPaused true if paused, false if active
   */
  public ClusterConfig(ClusterId id, Map<ResourceId, ResourceConfig> resourceMap,
      Map<ParticipantId, ParticipantConfig> participantMap,
      Map<ConstraintType, ClusterConstraints> constraintMap, UserConfig userConfig, boolean isPaused) {
    _id = id;
    _resourceMap = ImmutableMap.copyOf(resourceMap);
    _participantMap = ImmutableMap.copyOf(participantMap);
    _constraintMap = ImmutableMap.copyOf(constraintMap);
    _userConfig = userConfig;
    _isPaused = isPaused;
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
  public Map<ResourceId, ResourceConfig> getResourceMap() {
    return _resourceMap;
  }

  /**
   * Get all the constraints on the cluster
   * @return map of constraint type to constraints
   */
  public Map<ConstraintType, ClusterConstraints> getConstraintMap() {
    return _constraintMap;
  }

  /**
   * Get participants of the cluster
   * @return a map of participant id to participant, or empty map if none
   */
  public Map<ParticipantId, ParticipantConfig> getParticipantMap() {
    return _participantMap;
  }

  /**
   * Get user-specified configuration properties of this cluster
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _userConfig;
  }

  /**
   * Check the pasued status of the cluster
   * @return true if paused, false otherwise
   */
  public boolean isPaused() {
    return _isPaused;
  }

  /**
   * Assembles a cluster configuration
   */
  public static class Builder {
    private final ClusterId _id;
    private final Map<ResourceId, ResourceConfig> _resourceMap;
    private final Map<ParticipantId, ParticipantConfig> _participantMap;
    private final Map<ConstraintType, ClusterConstraints> _constraintMap;
    private UserConfig _userConfig;
    private boolean _isPaused;

    /**
     * Initialize builder for a cluster
     * @param id cluster id
     */
    public Builder(ClusterId id) {
      _id = id;
      _resourceMap = new HashMap<ResourceId, ResourceConfig>();
      _participantMap = new HashMap<ParticipantId, ParticipantConfig>();
      _constraintMap = new HashMap<ConstraintType, ClusterConstraints>();
      _isPaused = false;
      _userConfig = new UserConfig(id);
    }

    /**
     * Add a resource to the cluster
     * @param resource resource configuration
     * @return Builder
     */
    public Builder addResource(ResourceConfig resource) {
      _resourceMap.put(resource.getId(), resource);
      return this;
    }

    /**
     * Add multiple resources to the cluster
     * @param resources resource configurations
     * @return Builder
     */
    public Builder addResources(Collection<ResourceConfig> resources) {
      for (ResourceConfig resource : resources) {
        addResource(resource);
      }
      return this;
    }

    /**
     * Add a participant to the cluster
     * @param participant participant configuration
     * @return Builder
     */
    public Builder addParticipant(ParticipantConfig participant) {
      _participantMap.put(participant.getId(), participant);
      return this;
    }

    /**
     * Add multiple participants to the cluster
     * @param participants participant configurations
     * @return Builder
     */
    public Builder addParticipants(Collection<ParticipantConfig> participants) {
      for (ParticipantConfig participant : participants) {
        addParticipant(participant);
      }
      return this;
    }

    /**
     * Add a constraint to the cluster
     * @param constraint cluster constraint of a specific type
     * @return Builder
     */
    public Builder addConstraint(ClusterConstraints constraint) {
      _constraintMap.put(constraint.getType(), constraint);
      return this;
    }

    /**
     * Add multiple constraints to the cluster
     * @param constraints cluster constraints of multiple distinct types
     * @return Builder
     */
    public Builder addConstraints(Collection<ClusterConstraints> constraints) {
      for (ClusterConstraints constraint : constraints) {
        addConstraint(constraint);
      }
      return this;
    }

    /**
     * Set the paused status of the cluster
     * @param isPaused true if paused, false otherwise
     * @return Builder
     */
    public Builder setPausedStatus(boolean isPaused) {
      _isPaused = isPaused;
      return this;
    }

    /**
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Builder
     */
    public Builder userConfig(UserConfig userConfig) {
      _userConfig = userConfig;
      return this;
    }

    /**
     * Create the cluster configuration
     * @return ClusterConfig
     */
    public ClusterConfig build() {
      return new ClusterConfig(_id, _resourceMap, _participantMap, _constraintMap, _userConfig,
          _isPaused);
    }
  }
}
