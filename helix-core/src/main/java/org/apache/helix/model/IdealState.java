package org.apache.helix.model;

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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.State;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.controller.rebalancer.CustomRebalancer;
import org.apache.helix.controller.rebalancer.FullAutoRebalancer;
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.rebalancer.RebalancerRef;
import org.apache.helix.controller.rebalancer.SemiAutoRebalancer;
import org.apache.helix.task.FixedTargetTaskRebalancer;
import org.apache.helix.task.GenericTaskRebalancer;
import org.apache.log4j.Logger;

import com.google.common.base.Enums;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

/**
 * The ideal states of all partitions in a resource
 */
public class IdealState extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(IdealState.class);

  /**
   * Properties that are persisted and are queryable for an ideal state
   */
  public enum IdealStateProperty {
    NUM_PARTITIONS,
    STATE_MODEL_DEF_REF,
    STATE_MODEL_FACTORY_NAME,
    REPLICAS,
    @Deprecated
    IDEAL_STATE_MODE,
    REBALANCE_MODE,
    REBALANCE_TIMER_PERIOD,
    MAX_PARTITIONS_PER_INSTANCE,
    INSTANCE_GROUP_TAG,
    REBALANCER_CLASS_NAME,
    REBALANCER_CONFIG_NAME,
    HELIX_ENABLED
  }

  public static final String QUERY_LIST = "PREFERENCE_LIST_QUERYS";

  /**
   * Deprecated.
   * @see {@link RebalanceMode}
   */
  @Deprecated
  public enum IdealStateModeProperty {
    AUTO,
    CUSTOMIZED,
    AUTO_REBALANCE
  }

  /**
   * The mode used for rebalance. FULL_AUTO does both node location calculation and state
   * assignment, SEMI_AUTO only does the latter, and CUSTOMIZED does neither. USER_DEFINED
   * uses a Rebalancer implementation plugged in by the user.
   */
  public enum RebalanceMode {
    FULL_AUTO,
    SEMI_AUTO,
    CUSTOMIZED,
    USER_DEFINED,
    TASK,
    NONE
  }

  private static final Logger logger = Logger.getLogger(IdealState.class.getName());

  /**
   * Instantiate an ideal state for a resource
   * @param resourceName the name of the resource
   */
  public IdealState(String resourceName) {
    super(resourceName);
  }

  /**
   * Instantiate an ideal state for a resource
   * @param resourceId the id of the resource
   */
  public IdealState(ResourceId resourceId) {
    super(resourceId.stringify());
  }

  /**
   * Instantiate an ideal state from a record
   * @param record ZNRecord corresponding to an ideal state
   */
  public IdealState(ZNRecord record) {
    super(record);
  }

  /**
   * Get the associated resource
   * @return the name of the resource
   */
  public String getResourceName() {
    return _record.getId();
  }

  /**
   * Get the associated resource
   * @return the id of the resource
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getResourceName());
  }

  /**
   * Get the rebalance mode of the ideal state
   * @param mode {@link IdealStateModeProperty}
   */
  @Deprecated
  public void setIdealStateMode(String mode) {
    _record.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), mode);
    RebalanceMode rebalanceMode = normalizeRebalanceMode(IdealStateModeProperty.valueOf(mode));
    _record.setEnumField(IdealStateProperty.REBALANCE_MODE.toString(), rebalanceMode);
  }

  /**
   * Get the rebalance mode of the resource
   * @param rebalancerType
   */
  public void setRebalanceMode(RebalanceMode rebalancerType) {
    _record.setEnumField(IdealStateProperty.REBALANCE_MODE.toString(), rebalancerType);
    IdealStateModeProperty idealStateMode = denormalizeRebalanceMode(rebalancerType);
    _record.setEnumField(IdealStateProperty.IDEAL_STATE_MODE.toString(), idealStateMode);
  }

  /**
   * Get the maximum number of partitions an instance can serve
   * @return the partition capacity of an instance for this resource, or Integer.MAX_VALUE
   */
  public int getMaxPartitionsPerInstance() {
    return _record.getIntField(IdealStateProperty.MAX_PARTITIONS_PER_INSTANCE.toString(),
        Integer.MAX_VALUE);
  }

  /**
   * Define a custom rebalancer that implements {@link HelixRebalancer}
   * @param rebalancerClassName the name of the custom rebalancing class
   */
  public void setRebalancerClassName(String rebalancerClassName) {
    _record
        .setSimpleField(IdealStateProperty.REBALANCER_CLASS_NAME.toString(), rebalancerClassName);
  }

  /**
   * Get the name of the user-defined rebalancer associated with this resource
   * @return the rebalancer class name, or null if none is being used
   */
  public String getRebalancerClassName() {
    return _record.getSimpleField(IdealStateProperty.REBALANCER_CLASS_NAME.toString());
  }

  /**
   * Set a reference to the user-defined rebalancer associated with this resource(if any)
   * @param rebalancerRef a reference to a user-defined rebalancer
   */
  public void setRebalancerRef(RebalancerRef rebalancerRef) {
    if (rebalancerRef != null) {
      setRebalancerClassName(rebalancerRef.toString());
    } else {
      setRebalancerClassName(null);
    }
  }

  /**
   * Get a reference to the user-defined rebalancer associated with this resource(if any)
   * @return RebalancerRef
   */
  public RebalancerRef getRebalancerRef() {
    RebalancerRef ref = null;
    String className = getRebalancerClassName();
    if (className != null) {
      ref = RebalancerRef.from(getRebalancerClassName());
    } else {
      switch (getRebalanceMode()) {
      case FULL_AUTO:
        ref = RebalancerRef.from(FullAutoRebalancer.class);
        break;
      case SEMI_AUTO:
        ref = RebalancerRef.from(SemiAutoRebalancer.class);
        break;
      case CUSTOMIZED:
        ref = RebalancerRef.from(CustomRebalancer.class);
        break;
      default:
        break;
      }
    }
    return ref;
  }

  /**
   * Set the maximum number of partitions of this resource that an instance can serve
   * @param max the maximum number of partitions supported
   */
  public void setMaxPartitionsPerInstance(int max) {
    _record.setIntField(IdealStateProperty.MAX_PARTITIONS_PER_INSTANCE.toString(), max);
  }

  /**
   * Get the rebalancing mode on this resource
   * @return {@link IdealStateModeProperty}
   */
  @Deprecated
  public IdealStateModeProperty getIdealStateMode() {
    return _record.getEnumField(IdealStateProperty.IDEAL_STATE_MODE.toString(),
        IdealStateModeProperty.class, IdealStateModeProperty.AUTO);
  }

  /**
   * Get the rebalancing mode on this resource
   * @return {@link RebalanceMode}
   */
  public RebalanceMode getRebalanceMode() {
    RebalanceMode property =
        _record.getEnumField(IdealStateProperty.REBALANCE_MODE.toString(), RebalanceMode.class,
            RebalanceMode.NONE);
    if (property == RebalanceMode.NONE) {
      property = normalizeRebalanceMode(getIdealStateMode());
      setRebalanceMode(property);
    }
    return property;
  }

  /**
   * Set the preferred instance placement and state for a partition replica
   * @param partitionName the replica to set
   * @param instanceName the assigned instance
   * @param state the replica state in this instance
   */
  public void setPartitionState(String partitionName, String instanceName, String state) {
    Map<String, String> mapField = _record.getMapField(partitionName);
    if (mapField == null) {
      _record.setMapField(partitionName, new TreeMap<String, String>());
    }
    _record.getMapField(partitionName).put(instanceName, state);
  }

  /**
   * Set the preferred participant placement and state for a partition replica
   * @param partitionId the replica to set
   * @param participantId the assigned participant
   * @param state the replica state in this instance
   */
  public void setPartitionState(PartitionId partitionId, ParticipantId participantId, State state) {
    Map<String, String> mapField = _record.getMapField(partitionId.stringify());
    if (mapField == null) {
      _record.setMapField(partitionId.stringify(), new TreeMap<String, String>());
    }
    _record.getMapField(partitionId.stringify()).put(participantId.stringify(), state.toString());
  }

  /**
   * Get all of the partitions
   * @return a set of partition names
   */
  public Set<String> getPartitionSet() {
    switch (getRebalanceMode()) {
    case SEMI_AUTO:
    case FULL_AUTO:
      return _record.getListFields().keySet();
    case CUSTOMIZED:
    case USER_DEFINED:
    case TASK:
      return _record.getMapFields().keySet();
    default:
      logger.error("Invalid ideal state mode:" + getResourceName());
      return Collections.emptySet();
    }
  }

  /**
   * Get all of the partitions
   * @return a set of partitions
   */
  public Set<PartitionId> getPartitionIdSet() {
    Set<PartitionId> partitionSet = Sets.newHashSet();
    for (String partitionName : getPartitionSet()) {
      partitionSet.add(PartitionId.from(partitionName));
    }
    return partitionSet;
  }

  /**
   * Get the current mapping of a partition
   * @param partitionName the name of the partition
   * @return the instances where the replicas live and the state of each
   */
  public Map<String, String> getInstanceStateMap(String partitionName) {
    return _record.getMapField(partitionName);
  }

  /**
   * Set the current mapping of a partition
   * @param partitionName the partition to set
   * @param instanceStateMap (participant name, state) pairs
   */
  public void setInstanceStateMap(String partitionName, Map<String, String> instanceStateMap) {
    _record.setMapField(partitionName, instanceStateMap);
  }

  /**
   * Set the current mapping of a partition
   * @param partitionId the partition to set
   * @param participantStateMap (participant id, state) pairs
   */
  public void setParticipantStateMap(PartitionId partitionId,
      Map<ParticipantId, State> participantStateMap) {
    Map<String, String> rawMap = new HashMap<String, String>();
    for (ParticipantId participantId : participantStateMap.keySet()) {
      rawMap.put(participantId.stringify(), participantStateMap.get(participantId).toString());
    }
    _record.setMapField(partitionId.stringify(), rawMap);
  }

  /**
   * Get the current mapping of a partition
   * @param partitionId the name of the partition
   * @return the instances where the replicas live and the state of each
   */
  public Map<ParticipantId, State> getParticipantStateMap(PartitionId partitionId) {
    Map<String, String> instanceStateMap = getInstanceStateMap(partitionId.stringify());
    Map<ParticipantId, State> participantStateMap = Maps.newHashMap();
    if (instanceStateMap != null) {
      for (String participantId : instanceStateMap.keySet()) {
        participantStateMap.put(ParticipantId.from(participantId),
            State.from(instanceStateMap.get(participantId)));
      }
      return participantStateMap;
    }
    return null;
  }

  /**
   * Get the instances who host replicas of a partition
   * @param partitionName the partition to look up
   * @return set of instance names
   */
  public Set<String> getInstanceSet(String partitionName) {
    RebalanceMode rebalanceMode = getRebalanceMode();
    if (rebalanceMode == RebalanceMode.SEMI_AUTO || rebalanceMode == RebalanceMode.FULL_AUTO) {
      // get instances from list fields
      List<String> prefList = _record.getListField(partitionName);
      if (prefList != null) {
        return new TreeSet<String>(prefList);
      } else {
        logger.warn(partitionName + " does NOT exist");
        return Collections.emptySet();
      }
    } else if (rebalanceMode == RebalanceMode.CUSTOMIZED
        || rebalanceMode == RebalanceMode.USER_DEFINED || rebalanceMode == RebalanceMode.TASK) {
      // get instances from map fields
      Map<String, String> stateMap = _record.getMapField(partitionName);
      if (stateMap != null) {
        return new TreeSet<String>(stateMap.keySet());
      } else {
        logger.warn(partitionName + " does NOT exist");
        return Collections.emptySet();
      }
    } else {
      logger.error("Invalid ideal state mode: " + getResourceName());
      return Collections.emptySet();
    }
  }

  /**
   * Get the participants who host replicas of a partition
   * @param partitionId the partition to look up
   * @return set of participant ids
   */
  public Set<ParticipantId> getParticipantSet(PartitionId partitionId) {
    Set<ParticipantId> participantSet = Sets.newHashSet();
    for (String participantName : getInstanceSet(partitionId.stringify())) {
      participantSet.add(ParticipantId.from(participantName));
    }
    return participantSet;
  }

  /**
   * Set the preference list of a partition
   * @param partitionName the name of the partition to set
   * @param preferenceList a list of participants that can serve replicas of the partition
   */
  public void setPreferenceList(String partitionName, List<String> preferenceList) {
    _record.setListField(partitionName, preferenceList);
  }

  /**
   * Set the preference list of a partition
   * @param partitionId the id of the partition to set
   * @param preferenceList a list of participants that can serve replicas of the partition
   */
  public void setPreferenceList(PartitionId partitionId, List<ParticipantId> preferenceList) {
    if (preferenceList == null) {
      preferenceList = Collections.emptyList();
    }
    List<String> rawPreferenceList = new ArrayList<String>();
    for (ParticipantId participantId : preferenceList) {
      rawPreferenceList.add(participantId.stringify());
    }
    _record.setListField(partitionId.stringify(), rawPreferenceList);
  }

  /**
   * Get the preference list of a partition
   * @param partitionName the name of the partition
   * @return a list of instances that can serve replicas of the partition
   */
  public List<String> getPreferenceList(String partitionName) {
    List<String> instanceStateList = _record.getListField(partitionName);

    if (instanceStateList != null) {
      return instanceStateList;
    }
    logger.warn("Resource key:" + partitionName + " does not have a pre-computed preference list.");
    return null;
  }

  /**
   * Get the preference list of a partition
   * @param partitionId the partition id
   * @return an ordered list of participants that can serve replicas of the partition
   */
  public List<ParticipantId> getPreferenceList(PartitionId partitionId) {
    List<ParticipantId> preferenceList = Lists.newArrayList();
    List<String> preferenceStringList = getPreferenceList(partitionId.stringify());
    if (preferenceStringList != null) {
      for (String participantName : preferenceStringList) {
        preferenceList.add(ParticipantId.from(participantName));
      }
      return preferenceList;
    }
    return null;
  }

  /**
   * Get the state model associated with this resource
   * @return an identifier of the state model
   */
  public String getStateModelDefRef() {
    return _record.getSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString());
  }

  /**
   * Get the state model associated with this resource
   * @return an identifier of the state model
   */
  public StateModelDefId getStateModelDefId() {
    return StateModelDefId.from(getStateModelDefRef());
  }

  /**
   * Set the state model associated with this resource
   * @param stateModel state model identifier
   */
  public void setStateModelDefRef(String stateModel) {
    _record.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), stateModel);
  }

  /**
   * Set the state model associated with this resource
   * @param stateModel state model identifier
   */
  public void setStateModelDefId(StateModelDefId stateModelDefId) {
    setStateModelDefRef(stateModelDefId.stringify());
  }

  /**
   * Set the number of partitions of this resource
   * @param numPartitions the number of partitions
   */
  public void setNumPartitions(int numPartitions) {
    _record.setIntField(IdealStateProperty.NUM_PARTITIONS.toString(), numPartitions);
  }

  /**
   * Get the number of partitions of this resource
   * @return the number of partitions
   */
  public int getNumPartitions() {
    return _record.getIntField(IdealStateProperty.NUM_PARTITIONS.toString(), -1);
  }

  /**
   * Set the number of replicas for each partition of this resource. There are documented special
   * values for the replica count, so this is a String.
   * @param replicas replica count (as a string)
   */
  public void setReplicas(String replicas) {
    _record.setSimpleField(IdealStateProperty.REPLICAS.toString(), replicas);
  }

  /**
   * Get the number of replicas for each partition of this resource
   * @return number of replicas (as a string)
   */
  public String getReplicas() {
    // HACK: if replica doesn't exists, use the length of the first list field
    // instead
    // TODO: remove it when Dbus fixed the IdealState writer
    String replica = _record.getSimpleField(IdealStateProperty.REPLICAS.toString());
    if (replica == null) {
      String firstPartition = null;
      switch (getRebalanceMode()) {
      case SEMI_AUTO:
        if (_record.getListFields().size() == 0) {
          replica = "0";
        } else {
          firstPartition = new ArrayList<String>(_record.getListFields().keySet()).get(0);
          replica =
              Integer.toString(firstPartition == null ? 0 : _record.getListField(firstPartition)
                  .size());
        }
        logger
            .warn("could NOT find number of replicas in idealState. Use size of the first list instead. replica: "
                + replica + ", 1st partition: " + firstPartition);
        break;
      case CUSTOMIZED:
        if (_record.getMapFields().size() == 0) {
          replica = "0";
        } else {
          firstPartition = new ArrayList<String>(_record.getMapFields().keySet()).get(0);
          replica =
              Integer.toString(firstPartition == null ? 0 : _record.getMapField(firstPartition)
                  .size());
        }
        logger
            .warn("could NOT find replicas in idealState. Use size of the first map instead. replica: "
                + replica + ", 1st partition: " + firstPartition);
        break;
      default:
        replica = "0";
        logger.warn("could NOT determine replicas. set to 0");
        break;
      }
    }

    return replica;
  }

  /**
   * Set the state model factory associated with this resource
   * @param name state model factory name
   */
  public void setStateModelFactoryName(String name) {
    _record.setSimpleField(IdealStateProperty.STATE_MODEL_FACTORY_NAME.toString(), name);
  }

  /**
   * Set the state model factory associated with this resource
   * @param name state model factory id
   */
  public void setStateModelFactoryId(StateModelFactoryId stateModelFactoryId) {
    if (stateModelFactoryId != null) {
      setStateModelFactoryName(stateModelFactoryId.stringify());
    }
  }

  /**
   * Get the state model factory associated with this resource
   * @return state model factory name
   */
  public String getStateModelFactoryName() {
    return _record.getStringField(IdealStateProperty.STATE_MODEL_FACTORY_NAME.toString(),
        HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  /**
   * Get the state model factory associated with this resource
   * @return state model factory id
   */
  public StateModelFactoryId getStateModelFactoryId() {
    return StateModelFactoryId.from(getStateModelFactoryName());
  }

  /**
   * Set the frequency with which to rebalance
   * @return the rebalancing timer period
   */
  public int getRebalanceTimerPeriod() {
    return _record.getIntField(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString(), -1);
  }

  @Override
  public boolean isValid() {
    if (getNumPartitions() < 0) {
      logger.error("idealState:" + _record + " does not have number of partitions (was "
          + getNumPartitions() + ").");
      return false;
    }

    if (getStateModelDefRef() == null) {
      logger.error("idealStates:" + _record + " does not have state model definition.");
      return false;
    }

    if (getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
      String replicaStr = getReplicas();
      if (replicaStr == null) {
        logger.error("invalid ideal-state. missing replicas in auto mode. record was: " + _record);
        return false;
      }

      if (!replicaStr.equals(HelixConstants.StateModelToken.ANY_LIVEINSTANCE.toString())) {
        int replica = Integer.parseInt(replicaStr);
        Set<String> partitionSet = getPartitionSet();
        for (String partition : partitionSet) {
          List<String> preferenceList = getPreferenceList(partition);
          if (preferenceList == null || preferenceList.size() != replica) {
            logger
                .error("invalid ideal-state. preference-list size not equals to replicas in auto mode. replica: "
                    + replica
                    + ", preference-list size: "
                    + preferenceList.size()
                    + ", record was: " + _record);
            return false;
          }
        }
      }
    }

    return true;
  }

  /**
   * Set a tag to check to enforce assignment to certain instances
   * @param groupTag the instance group tag
   */
  public void setInstanceGroupTag(String groupTag) {
    _record.setSimpleField(IdealStateProperty.INSTANCE_GROUP_TAG.toString(), groupTag);
  }

  /**
   * Check for a tag that will restrict assignment to instances with a matching tag
   * @return the group tag, or null if none is present
   */
  public String getInstanceGroupTag() {
    return _record.getSimpleField(IdealStateProperty.INSTANCE_GROUP_TAG.toString());
  }

  /**
   * Update the ideal state from a ResourceAssignment computed during a rebalance
   * @param assignment the new resource assignment
   * @param stateModelDef state model of the resource
   */
  public void updateFromAssignment(ResourceAssignment assignment, StateModelDefinition stateModelDef) {
    // clear all preference lists and maps
    _record.getMapFields().clear();
    _record.getListFields().clear();

    // assign a partition at a time
    for (PartitionId partition : assignment.getMappedPartitionIds()) {
      List<ParticipantId> preferenceList = new ArrayList<ParticipantId>();
      Map<ParticipantId, State> participantStateMap = new HashMap<ParticipantId, State>();

      // invert the map to get in state order
      Map<ParticipantId, State> replicaMap = assignment.getReplicaMap(partition);
      ListMultimap<State, ParticipantId> inverseMap = ArrayListMultimap.create();
      Multimaps.invertFrom(Multimaps.forMap(replicaMap), inverseMap);

      // update the ideal state in order of state priorities
      for (State state : stateModelDef.getTypedStatesPriorityList()) {
        if (!state.equals(State.from(HelixDefinedState.DROPPED))
            && !state.equals(State.from(HelixDefinedState.ERROR))) {
          List<ParticipantId> stateParticipants = inverseMap.get(state);
          for (ParticipantId participant : stateParticipants) {
            preferenceList.add(participant);
            participantStateMap.put(participant, state);
          }
        }
      }
      setPreferenceList(partition, preferenceList);
      setParticipantStateMap(partition, participantStateMap);
    }
  }

  private RebalanceMode normalizeRebalanceMode(IdealStateModeProperty mode) {
    RebalanceMode property;
    switch (mode) {
    case AUTO_REBALANCE:
      property = RebalanceMode.FULL_AUTO;
      break;
    case AUTO:
      property = RebalanceMode.SEMI_AUTO;
      break;
    case CUSTOMIZED:
      property = RebalanceMode.CUSTOMIZED;
      break;
    default:
      String rebalancerName = getRebalancerClassName();
      if (rebalancerName == null) {
        property = RebalanceMode.SEMI_AUTO;
      } else if (rebalancerName.equals(FixedTargetTaskRebalancer.class.getName())
          || rebalancerName.equals(GenericTaskRebalancer.class.getName())) {
        property = RebalanceMode.TASK;
      } else {
        property = RebalanceMode.USER_DEFINED;
      }
      break;
    }
    return property;
  }

  private IdealStateModeProperty denormalizeRebalanceMode(RebalanceMode rebalancerType) {
    IdealStateModeProperty property;
    switch (rebalancerType) {
    case FULL_AUTO:
      property = IdealStateModeProperty.AUTO_REBALANCE;
      break;
    case SEMI_AUTO:
      property = IdealStateModeProperty.AUTO;
      break;
    case CUSTOMIZED:
      property = IdealStateModeProperty.CUSTOMIZED;
      break;
    default:
      property = IdealStateModeProperty.AUTO;
      break;
    }
    return property;
  }

  /**
   * Parse a rebalance mode from a string. It can also understand IdealStateModeProperty values
   * @param mode string containing a RebalanceMode value
   * @param defaultMode the mode to use if the string is not valid
   * @return converted RebalanceMode value
   */
  public RebalanceMode rebalanceModeFromString(String mode, RebalanceMode defaultMode) {
    RebalanceMode rebalanceMode = defaultMode;
    try {
      rebalanceMode = RebalanceMode.valueOf(mode);
    } catch (Exception rebalanceModeException) {
      try {
        IdealStateModeProperty oldMode = IdealStateModeProperty.valueOf(mode);
        rebalanceMode = normalizeRebalanceMode(oldMode);
      } catch (Exception e) {
        logger.error(e);
      }
    }
    return rebalanceMode;
  }

  /**
   * Get the non-Helix simple fields from this property and add them to a UserConfig
   * @param userConfig the user config to update
   */
  public void updateUserConfig(UserConfig userConfig) {
    try {
      for (String simpleField : _record.getSimpleFields().keySet()) {
        Optional<IdealStateProperty> enumField =
            Enums.getIfPresent(IdealStateProperty.class, simpleField);
        if (!simpleField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()) {
          userConfig.setSimpleField(simpleField, _record.getSimpleField(simpleField));
        }
      }
    } catch (NoSuchMethodError e) {
      LOG.error("Could not update user config", e);
    }
  }

  /**
   * Convert a preference list of strings into a preference list of participants
   * @param rawPreferenceList the list of strings representing participant names
   * @return converted list
   */
  public static List<ParticipantId> preferenceListFromStringList(List<String> rawPreferenceList) {
    if (rawPreferenceList == null) {
      return Collections.emptyList();
    }
    return Lists.transform(new ArrayList<String>(rawPreferenceList),
        new Function<String, ParticipantId>() {
          @Override
          public ParticipantId apply(String participantName) {
            return ParticipantId.from(participantName);
          }
        });
  }

  /**
   * Convert preference lists of strings into preference lists of participants
   * @param rawPreferenceLists a map of partition name to a list of participant names
   * @return converted lists as a map
   */
  public static Map<? extends PartitionId, List<ParticipantId>> preferenceListsFromStringLists(
      Map<String, List<String>> rawPreferenceLists) {
    if (rawPreferenceLists == null) {
      return Collections.emptyMap();
    }
    Map<PartitionId, List<ParticipantId>> preferenceLists =
        new HashMap<PartitionId, List<ParticipantId>>();
    for (String partitionId : rawPreferenceLists.keySet()) {
      preferenceLists.put(PartitionId.from(partitionId),
          preferenceListFromStringList(rawPreferenceLists.get(partitionId)));
    }
    return preferenceLists;
  }

  /**
   * Convert a preference list of participants into a preference list of strings
   * @param preferenceList the list of strings representing participant ids
   * @return converted list
   */
  public static List<String> stringListFromPreferenceList(List<ParticipantId> preferenceList) {
    if (preferenceList == null) {
      return Collections.emptyList();
    }
    return Lists.transform(new ArrayList<ParticipantId>(preferenceList),
        new Function<ParticipantId, String>() {
          @Override
          public String apply(ParticipantId participantId) {
            return participantId.stringify();
          }
        });
  }

  /**
   * Convert preference lists of participants into preference lists of strings
   * @param preferenceLists a map of partition id to a list of participant ids
   * @return converted lists as a map
   */
  public static Map<String, List<String>> stringListsFromPreferenceLists(
      Map<PartitionId, List<ParticipantId>> preferenceLists) {
    if (preferenceLists == null) {
      return Collections.emptyMap();
    }
    Map<String, List<String>> rawPreferenceLists = new HashMap<String, List<String>>();
    for (PartitionId partitionId : preferenceLists.keySet()) {
      rawPreferenceLists.put(partitionId.stringify(),
          stringListFromPreferenceList(preferenceLists.get(partitionId)));
    }
    return rawPreferenceLists;
  }

  /**
   * Convert a partition mapping as strings into a participant state map
   * @param rawMap the map of participant name to state
   * @return converted map
   */
  public static Map<ParticipantId, State> participantStateMapFromStringMap(
      Map<String, String> rawMap) {
    return ResourceAssignment.replicaMapFromStringMap(rawMap);
  }

  /**
   * Convert a full state mapping as strings into participant state maps
   * @param rawMaps the map of partition name to participant name and state
   * @return converted maps
   */
  public static Map<? extends PartitionId, Map<ParticipantId, State>> participantStateMapsFromStringMaps(
      Map<String, Map<String, String>> rawMaps) {
    return ResourceAssignment.replicaMapsFromStringMaps(rawMaps);
  }

  /**
   * Convert a partition mapping into a mapping of string names
   * @param participantStateMap the map of participant id to state
   * @return converted map
   */
  public static Map<String, String> stringMapFromParticipantStateMap(
      Map<ParticipantId, State> participantStateMap) {
    return ResourceAssignment.stringMapFromReplicaMap(participantStateMap);
  }

  /**
   * Convert a full state mapping into a mapping of string names
   * @param participantStateMaps the map of partition id to participant id and state
   * @return converted maps
   */
  public static Map<String, Map<String, String>> stringMapsFromParticipantStateMaps(
      Map<PartitionId, Map<ParticipantId, State>> participantStateMaps) {
    return ResourceAssignment.stringMapsFromReplicaMaps(participantStateMaps);
  }

  /**
   * Get if the resource is enabled or not
   * By default, it's enabled
   * @return true if enabled; false otherwise
   */
  public boolean isEnabled() {
    return _record.getBooleanField(IdealStateProperty.HELIX_ENABLED.name(), true);
  }

  /**
   * Enable/Disable the resource
   * @param enabled
   */
  public void enable(boolean enabled) {
    _record.setSimpleField(IdealStateProperty.HELIX_ENABLED.name(), Boolean.toString(enabled));
  }
}
