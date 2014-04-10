package org.apache.helix.api.config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.Scope;
import org.apache.helix.api.State;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ConstraintId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.model.ClusterConstraints;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.ClusterConstraints.ConstraintValue;
import org.apache.helix.model.ConstraintItem;
import org.apache.helix.model.Message.MessageType;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.Transition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

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
  private static final Logger LOG = Logger.getLogger(ClusterConfig.class);

  private final ClusterId _id;
  private final Map<ResourceId, ResourceConfig> _resourceMap;
  private final Map<ParticipantId, ParticipantConfig> _participantMap;
  private final Map<ConstraintType, ClusterConstraints> _constraintMap;
  private final Map<StateModelDefId, StateModelDefinition> _stateModelMap;
  private final UserConfig _userConfig;
  private final boolean _isPaused;
  private final boolean _autoJoin;

  /**
   * Initialize a cluster configuration. Also see ClusterConfig.Builder
   * @param id cluster id
   * @param resourceMap map of resource id to resource config
   * @param participantMap map of participant id to participant config
   * @param constraintMap map of constraint type to all constraints of that type
   * @param stateModelMap map of state model id to state model definition
   * @param userConfig user-defined cluster properties
   * @param isPaused true if paused, false if active
   * @param allowAutoJoin true if participants can join automatically, false otherwise
   */
  private ClusterConfig(ClusterId id, Map<ResourceId, ResourceConfig> resourceMap,
      Map<ParticipantId, ParticipantConfig> participantMap,
      Map<ConstraintType, ClusterConstraints> constraintMap,
      Map<StateModelDefId, StateModelDefinition> stateModelMap, UserConfig userConfig,
      boolean isPaused, boolean allowAutoJoin) {
    _id = id;
    _resourceMap = ImmutableMap.copyOf(resourceMap);
    _participantMap = ImmutableMap.copyOf(participantMap);
    _constraintMap = ImmutableMap.copyOf(constraintMap);
    _stateModelMap = ImmutableMap.copyOf(stateModelMap);
    _userConfig = userConfig;
    _isPaused = isPaused;
    _autoJoin = allowAutoJoin;
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
   * Get the maximum number of participants that can be in a state
   * @param scope the scope for the bound
   * @param stateModelDefId the state model of the state
   * @param state the constrained state
   * @return The upper bound, which can be "-1" if unspecified, a numerical upper bound, "R" for
   *         number of replicas, or "N" for number of participants
   */
  public String getStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
      State state) {
    // set up attributes to match based on the scope
    ClusterConstraints stateConstraints = getConstraintMap().get(ConstraintType.STATE_CONSTRAINT);
    Map<ConstraintAttribute, String> matchAttributes = Maps.newHashMap();
    matchAttributes.put(ConstraintAttribute.STATE, state.toString());
    matchAttributes.put(ConstraintAttribute.STATE_MODEL, stateModelDefId.toString());
    switch (scope.getType()) {
    case CLUSTER:
      // cluster is implicit
      break;
    case RESOURCE:
      matchAttributes.put(ConstraintAttribute.RESOURCE, scope.getScopedId().stringify());
      break;
    default:
      LOG.error("Unsupported scope for state constraint: " + scope);
      return "-1";
    }
    Set<ConstraintItem> matches = stateConstraints.match(matchAttributes);
    int value = -1;
    for (ConstraintItem item : matches) {
      // match: if an R or N is found, always choose that one
      // otherwise, take the minimum of the counts specified in the constraints
      String constraintValue = item.getConstraintValue();
      if (constraintValue != null) {
        if (constraintValue.equals(ConstraintValue.N.toString())
            || constraintValue.equals(ConstraintValue.R.toString())) {
          return constraintValue;
        } else {
          try {
            int current = Integer.parseInt(constraintValue);
            if (value == -1 || current < value) {
              value = current;
            }
          } catch (NumberFormatException e) {
            LOG.error("Invalid state upper bound: " + constraintValue);
          }
        }
      }
    }
    return Integer.toString(value);
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
    // set up attributes to match based on the scope
    ClusterConstraints transitionConstraints =
        getConstraintMap().get(ConstraintType.MESSAGE_CONSTRAINT);
    Map<ConstraintAttribute, String> matchAttributes = Maps.newHashMap();
    matchAttributes.put(ConstraintAttribute.STATE_MODEL, stateModelDefId.toString());
    matchAttributes.put(ConstraintAttribute.MESSAGE_TYPE, MessageType.STATE_TRANSITION.toString());
    matchAttributes.put(ConstraintAttribute.TRANSITION, transition.toString());
    switch (scope.getType()) {
    case CLUSTER:
      // cluster is implicit
      break;
    case RESOURCE:
      matchAttributes.put(ConstraintAttribute.RESOURCE, scope.getScopedId().stringify());
      break;
    case PARTICIPANT:
      matchAttributes.put(ConstraintAttribute.INSTANCE, scope.getScopedId().stringify());
      break;
    case PARTITION:
      matchAttributes.put(ConstraintAttribute.PARTITION, scope.getScopedId().stringify());
      break;
    default:
      LOG.error("Unsupported scope for transition constraints: " + scope);
      return Integer.MAX_VALUE;
    }
    Set<ConstraintItem> matches = transitionConstraints.match(matchAttributes);
    int value = Integer.MAX_VALUE;
    for (ConstraintItem item : matches) {
      String constraintValue = item.getConstraintValue();
      if (constraintValue != null) {
        try {
          int current = Integer.parseInt(constraintValue);
          if (current < value) {
            value = current;
          }
        } catch (NumberFormatException e) {
          LOG.error("Invalid in-flight transition cap: " + constraintValue);
        }
      }
    }
    return value;
  }

  /**
   * Get participants of the cluster
   * @return a map of participant id to participant, or empty map if none
   */
  public Map<ParticipantId, ParticipantConfig> getParticipantMap() {
    return _participantMap;
  }

  /**
   * Get all the state model definitions on the cluster
   * @return map of state model definition id to state model definition
   */
  public Map<StateModelDefId, StateModelDefinition> getStateModelMap() {
    return _stateModelMap;
  }

  /**
   * Get user-specified configuration properties of this cluster
   * @return UserConfig properties
   */
  public UserConfig getUserConfig() {
    return _userConfig;
  }

  /**
   * Check the paused status of the cluster
   * @return true if paused, false otherwise
   */
  public boolean isPaused() {
    return _isPaused;
  }

  /**
   * Check if this cluster allows participants to join automatically
   * @return true if allowed, false if disallowed
   */
  public boolean autoJoinAllowed() {
    return _autoJoin;
  }

  /**
   * Update context for a ClusterConfig
   */
  public static class Delta {
    private enum Fields {
      USER_CONFIG,
      AUTO_JOIN
    }

    private Set<Fields> _updateFields;
    private Map<ConstraintType, Set<ConstraintId>> _removedConstraints;
    private Builder _builder;

    /**
     * Instantiate the delta for a cluster config
     * @param clusterId the cluster to update
     */
    public Delta(ClusterId clusterId) {
      _updateFields = Sets.newHashSet();
      _removedConstraints = Maps.newHashMap();
      for (ConstraintType type : ConstraintType.values()) {
        Set<ConstraintId> constraints = Sets.newHashSet();
        _removedConstraints.put(type, constraints);
      }
      _builder = new Builder(clusterId);
    }

    /**
     * Add a state upper bound constraint
     * @param scope scope under which the constraint is valid
     * @param stateModelDefId identifier of the state model that owns the state
     * @param state the state to constrain
     * @param upperBound maximum number of replicas per partition in the state
     * @return Delta
     */
    public Delta addStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        State state, int upperBound) {
      return addStateUpperBoundConstraint(scope, stateModelDefId, state,
          Integer.toString(upperBound));
    }

    /**
     * Add a state upper bound constraint
     * @param scope scope under which the constraint is valid
     * @param stateModelDefId identifier of the state model that owns the state
     * @param state the state to constrain
     * @param dynamicUpperBound the upper bound of replicas per partition in the state, can be a
     *          number, or the currently supported special bound values:<br />
     *          "R" - Refers to the number of replicas specified during resource
     *          creation. This allows having different replication factor for each
     *          resource without having to create a different state machine. <br />
     *          "N" - Refers to all nodes in the cluster. Useful for resources that need
     *          to exist on all nodes. This way one can add/remove nodes without having
     *          the change the bounds.
     * @return Delta
     */
    public Delta addStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        State state, String dynamicUpperBound) {
      _builder.addStateUpperBoundConstraint(scope, stateModelDefId, state, dynamicUpperBound);
      return this;
    }

    /**
     * Remove state upper bound constraint
     * @param scope scope under which the constraint is valid
     * @param stateModelDefId identifier of the state model that owns the state
     * @param state the state to constrain
     * @return Delta
     */
    public Delta removeStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        State state) {
      _removedConstraints.get(ConstraintType.STATE_CONSTRAINT).add(
          ConstraintId.from(scope, stateModelDefId, state));
      return this;
    }

    /**
     * Add a constraint on the maximum number of in-flight transitions of a certain type
     * @param scope scope of the constraint
     * @param stateModelDefId identifies the state model containing the transition
     * @param transition the transition to constrain
     * @param maxInFlightTransitions number of allowed in-flight transitions in the scope
     * @return Delta
     */
    public Delta addTransitionConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        Transition transition, int maxInFlightTransitions) {
      _builder.addTransitionConstraint(scope, stateModelDefId, transition, maxInFlightTransitions);
      return this;
    }

    /**
     * Remove a constraint on the maximum number of in-flight transitions of a certain type
     * @param scope scope of the constraint
     * @param stateModelDefId identifies the state model containing the transition
     * @param transition the transition to constrain
     * @return Delta
     */
    public Delta removeTransitionConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        Transition transition) {
      _removedConstraints.get(ConstraintType.MESSAGE_CONSTRAINT).add(
          ConstraintId.from(scope, stateModelDefId, transition));
      return this;
    }

    /**
     * Add a single constraint item
     * @param type type of the constraint item
     * @param constraintId unique constraint id
     * @param item instantiated ConstraintItem
     * @return Delta
     */
    public Delta addConstraintItem(ConstraintType type, ConstraintId constraintId,
        ConstraintItem item) {
      _builder.addConstraint(type, constraintId, item);
      return this;
    }

    /**
     * Remove a single constraint item
     * @param type type of the constraint item
     * @param constraintId unique constraint id
     * @return Delta
     */
    public Delta removeConstraintItem(ConstraintType type, ConstraintId constraintId) {
      _removedConstraints.get(type).add(constraintId);
      return this;
    }

    /*
     * Set the user configuration
     * @param userConfig user-specified properties
     * @return Delta
     */
    public Delta setUserConfig(UserConfig userConfig) {
      _builder.userConfig(userConfig);
      _updateFields.add(Fields.USER_CONFIG);
      return this;
    }

    /**
     * Allow or disallow participants from automatically being able to join the cluster
     * @param autoJoin true if allowed, false if disallowed
     * @return Delta
     */
    public Delta setAutoJoin(boolean autoJoin) {
      _builder.autoJoin(autoJoin);
      _updateFields.add(Fields.AUTO_JOIN);
      return this;
    }

    /**
     * Create a ClusterConfig that is the combination of an existing ClusterConfig and this delta
     * @param orig the original ClusterConfig
     * @return updated ClusterConfig
     */
    public ClusterConfig mergeInto(ClusterConfig orig) {
      // copy in original and updated fields
      ClusterConfig deltaConfig = _builder.build();
      Builder builder =
          new Builder(orig.getId()).addResources(orig.getResourceMap().values())
              .addParticipants(orig.getParticipantMap().values())
              .addStateModelDefinitions(orig.getStateModelMap().values())
              .userConfig(orig.getUserConfig()).pausedStatus(orig.isPaused())
              .autoJoin(orig.autoJoinAllowed());
      for (Fields field : _updateFields) {
        switch (field) {
        case USER_CONFIG:
          builder.userConfig(deltaConfig.getUserConfig());
          break;
        case AUTO_JOIN:
          builder.autoJoin(deltaConfig.autoJoinAllowed());
          break;
        }
      }
      // add constraint deltas
      for (ConstraintType type : ConstraintType.values()) {
        ClusterConstraints constraints;
        if (orig.getConstraintMap().containsKey(type)) {
          constraints = orig.getConstraintMap().get(type);
        } else {
          constraints = new ClusterConstraints(type);
        }
        // add new constraints
        if (deltaConfig.getConstraintMap().containsKey(type)) {
          ClusterConstraints deltaConstraints = deltaConfig.getConstraintMap().get(type);
          for (ConstraintId constraintId : deltaConstraints.getConstraintItems().keySet()) {
            ConstraintItem constraintItem = deltaConstraints.getConstraintItem(constraintId);
            constraints.addConstraintItem(constraintId, constraintItem);
          }
        }
        // remove constraints
        for (ConstraintId constraintId : _removedConstraints.get(type)) {
          constraints.removeConstraintItem(constraintId);
        }
        builder.addConstraint(constraints);
      }

      // get the result
      return builder.build();
    }
  }

  /**
   * Assembles a cluster configuration
   */
  public static class Builder {
    private final ClusterId _id;
    private final Map<ResourceId, ResourceConfig> _resourceMap;
    private final Map<ParticipantId, ParticipantConfig> _participantMap;
    private final Map<ConstraintType, ClusterConstraints> _constraintMap;
    private final Map<StateModelDefId, StateModelDefinition> _stateModelMap;
    private UserConfig _userConfig;
    private boolean _isPaused;
    private boolean _autoJoin;

    /**
     * Initialize builder for a cluster
     * @param id cluster id
     */
    public Builder(ClusterId id) {
      _id = id;
      _resourceMap = new HashMap<ResourceId, ResourceConfig>();
      _participantMap = new HashMap<ParticipantId, ParticipantConfig>();
      _constraintMap = new HashMap<ConstraintType, ClusterConstraints>();
      _stateModelMap = new HashMap<StateModelDefId, StateModelDefinition>();
      _isPaused = false;
      _autoJoin = false;
      _userConfig = new UserConfig(Scope.cluster(id));
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
      ClusterConstraints existConstraints = getConstraintsInstance(constraint.getType());
      for (ConstraintId constraintId : constraint.getConstraintItems().keySet()) {
        existConstraints
            .addConstraintItem(constraintId, constraint.getConstraintItem(constraintId));
      }
      return this;
    }

    /**
     * Add a single constraint item
     * @param type type of the constraint
     * @param constraintId unique constraint identifier
     * @param item instantiated ConstraintItem
     * @return Builder
     */
    public Builder addConstraint(ConstraintType type, ConstraintId constraintId, ConstraintItem item) {
      ClusterConstraints existConstraints = getConstraintsInstance(type);
      existConstraints.addConstraintItem(constraintId, item);
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
     * Add a constraint on the maximum number of in-flight transitions of a certain type
     * @param scope scope of the constraint
     * @param stateModelDefId identifies the state model containing the transition
     * @param transition the transition to constrain
     * @param maxInFlightTransitions number of allowed in-flight transitions in the scope
     * @return Builder
     */
    public Builder addTransitionConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        Transition transition, int maxInFlightTransitions) {
      Map<String, String> attributes = Maps.newHashMap();
      attributes.put(ConstraintAttribute.MESSAGE_TYPE.toString(),
          MessageType.STATE_TRANSITION.toString());
      attributes.put(ConstraintAttribute.CONSTRAINT_VALUE.toString(),
          Integer.toString(maxInFlightTransitions));
      attributes.put(ConstraintAttribute.TRANSITION.toString(), transition.toString());
      attributes.put(ConstraintAttribute.STATE_MODEL.toString(), stateModelDefId.stringify());
      switch (scope.getType()) {
      case CLUSTER:
        // cluster is implicit
        break;
      case RESOURCE:
        attributes.put(ConstraintAttribute.RESOURCE.toString(), scope.getScopedId().stringify());
        break;
      case PARTICIPANT:
        attributes.put(ConstraintAttribute.INSTANCE.toString(), scope.getScopedId().stringify());
        break;
      default:
        LOG.error("Unsupported scope for adding a transition constraint: " + scope);
        return this;
      }
      ConstraintItem item = new ConstraintItemBuilder().addConstraintAttributes(attributes).build();
      ClusterConstraints constraints = getConstraintsInstance(ConstraintType.MESSAGE_CONSTRAINT);
      constraints.addConstraintItem(ConstraintId.from(scope, stateModelDefId, transition), item);
      return this;
    }

    /**
     * Add a state upper bound constraint
     * @param scope scope under which the constraint is valid
     * @param stateModelDefId identifier of the state model that owns the state
     * @param state the state to constrain
     * @param upperBound maximum number of replicas per partition in the state
     * @return Builder
     */
    public Builder addStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        State state, int upperBound) {
      return addStateUpperBoundConstraint(scope, stateModelDefId, state,
          Integer.toString(upperBound));
    }

    /**
     * Add a state upper bound constraint
     * @param scope scope under which the constraint is valid
     * @param stateModelDefId identifier of the state model that owns the state
     * @param state the state to constrain
     * @param dynamicUpperBound the upper bound of replicas per partition in the state, can be a
     *          number, or the currently supported special bound values:<br />
     *          "R" - Refers to the number of replicas specified during resource
     *          creation. This allows having different replication factor for each
     *          resource without having to create a different state machine. <br />
     *          "N" - Refers to all nodes in the cluster. Useful for resources that need
     *          to exist on all nodes. This way one can add/remove nodes without having
     *          the change the bounds.
     * @return Builder
     */
    public Builder addStateUpperBoundConstraint(Scope<?> scope, StateModelDefId stateModelDefId,
        State state, String dynamicUpperBound) {
      Map<String, String> attributes = Maps.newHashMap();
      attributes.put(ConstraintAttribute.STATE.toString(), state.toString());
      attributes.put(ConstraintAttribute.STATE_MODEL.toString(), stateModelDefId.stringify());
      attributes.put(ConstraintAttribute.CONSTRAINT_VALUE.toString(), dynamicUpperBound);
      switch (scope.getType()) {
      case CLUSTER:
        // cluster is implicit
        break;
      case RESOURCE:
        attributes.put(ConstraintAttribute.RESOURCE.toString(), scope.getScopedId().stringify());
        break;
      default:
        LOG.error("Unsupported scope for adding a state constraint: " + scope);
        return this;
      }
      ConstraintItem item = new ConstraintItemBuilder().addConstraintAttributes(attributes).build();
      ClusterConstraints constraints = getConstraintsInstance(ConstraintType.STATE_CONSTRAINT);
      constraints.addConstraintItem(ConstraintId.from(scope, stateModelDefId, state), item);
      return this;
    }

    /**
     * Add a state model definition to the cluster
     * @param stateModelDef state model definition of the cluster
     * @return Builder
     */
    public Builder addStateModelDefinition(StateModelDefinition stateModelDef) {
      _stateModelMap.put(stateModelDef.getStateModelDefId(), stateModelDef);
      // add state constraints from the state model definition
      for (State state : stateModelDef.getTypedStatesPriorityList()) {
        if (!stateModelDef.getNumParticipantsPerState(state).equals("-1")) {
          addStateUpperBoundConstraint(Scope.cluster(_id), stateModelDef.getStateModelDefId(),
              state, stateModelDef.getNumParticipantsPerState(state));
        }
      }
      return this;
    }

    /**
     * Add multiple state model definitions
     * @param stateModelDefs collection of state model definitions for the cluster
     * @return Builder
     */
    public Builder addStateModelDefinitions(Collection<StateModelDefinition> stateModelDefs) {
      for (StateModelDefinition stateModelDef : stateModelDefs) {
        addStateModelDefinition(stateModelDef);
      }
      return this;
    }

    /**
     * Set the paused status of the cluster
     * @param isPaused true if paused, false otherwise
     * @return Builder
     */
    public Builder pausedStatus(boolean isPaused) {
      _isPaused = isPaused;
      return this;
    }

    /**
     * Allow or disallow participants from automatically being able to join the cluster
     * @param autoJoin true if allowed, false if disallowed
     * @return Builder
     */
    public Builder autoJoin(boolean autoJoin) {
      _autoJoin = autoJoin;
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
      return new ClusterConfig(_id, _resourceMap, _participantMap, _constraintMap, _stateModelMap,
          _userConfig, _isPaused, _autoJoin);
    }

    /**
     * Get a valid instance of ClusterConstraints for a type
     * @param type the type
     * @return ClusterConstraints
     */
    private ClusterConstraints getConstraintsInstance(ConstraintType type) {
      ClusterConstraints constraints = _constraintMap.get(type);
      if (constraints == null) {
        constraints = new ClusterConstraints(type);
        _constraintMap.put(type, constraints);
      }
      return constraints;
    }
  }
}
