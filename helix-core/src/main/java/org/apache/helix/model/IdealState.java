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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixProperty;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.model.ResourceConfig.ResourceConfigProperty;
import org.apache.helix.task.JobRebalancer;
import org.apache.helix.task.TaskRebalancer;
import org.apache.helix.task.WorkflowRebalancer;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ideal states of all partitions in a resource
 */
public class IdealState extends HelixProperty {
  /**
   * Properties that are persisted and are queryable for an ideal state
   * Deprecated, use ResourceConfig.ResourceConfigProperty instead.
   */
  @Deprecated
  public enum IdealStateProperty {
    NUM_PARTITIONS,
    STATE_MODEL_DEF_REF,
    STATE_MODEL_FACTORY_NAME,
    REPLICAS,
    MIN_ACTIVE_REPLICAS,
    REBALANCE_DELAY,
    @Deprecated
    DELAY_REBALANCE_DISABLED,
    @Deprecated
    IDEAL_STATE_MODE,
    REBALANCE_MODE,
    REBALANCER_CLASS_NAME,
    REBALANCE_TIMER_PERIOD,
    REBALANCE_STRATEGY,
    MAX_PARTITIONS_PER_INSTANCE,
    INSTANCE_GROUP_TAG,
    HELIX_ENABLED,
    RESOURCE_GROUP_NAME,
    RESOURCE_TYPE,
    GROUP_ROUTING_ENABLED,
    EXTERNAL_VIEW_DISABLED
  }

  public static final String QUERY_LIST = "PREFERENCE_LIST_QUERYS";
  public static final Set<String> LEGACY_TASK_REBALANCERS =
      new HashSet<>(Arrays.asList("org.apache.helix.task.GenericTaskRebalancer",
          "org.apache.helix.task.FixedTargetTaskRebalancer"));

  /**
   * Deprecated, use ResourceConfig.ResourceConfigConstants instead
   */
  @Deprecated
  public enum IdealStateConstants {
    ANY_LIVEINSTANCE
  }

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
   * uses a Rebalancer implementation plugged in by the user. TASK designates that a
   * {@link TaskRebalancer} instance should be used to rebalance this resource.
   */
  public enum RebalanceMode {
    FULL_AUTO,
    SEMI_AUTO,
    CUSTOMIZED,
    USER_DEFINED,
    TASK,
    NONE
  }

  private static final Logger logger = LoggerFactory.getLogger(IdealState.class.getName());

  /**
   * Instantiate an ideal state for a resource
   * @param resourceName the name of the resource
   */
  public IdealState(String resourceName) {
    super(resourceName);
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
   * Set the rebalance mode of the ideal state
   * @param mode {@link IdealStateModeProperty}
   */
  @Deprecated
  public void setIdealStateMode(String mode) {
    _record.setSimpleField(IdealStateProperty.IDEAL_STATE_MODE.toString(), mode);
    RebalanceMode rebalanceMode = normalizeRebalanceMode(IdealStateModeProperty.valueOf(mode));
    _record.setEnumField(IdealStateProperty.REBALANCE_MODE.toString(), rebalanceMode);
  }

  /**
   * Set the rebalance mode of the resource
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
   * Define a custom rebalancer that implements {@link Rebalancer}
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
   * Specify the strategy for Helix to use to compute the partition-instance assignment,
   * i,e, the custom rebalance strategy that implements {@link org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy}
   *
   * @param rebalanceStrategy
   * @return
   */
  public void setRebalanceStrategy(String rebalanceStrategy) {
    _record.setSimpleField(IdealStateProperty.REBALANCE_STRATEGY.name(), rebalanceStrategy);
  }

  /**
   * Get the rebalance strategy for this resource.
   *
   * @return rebalance strategy, or null if not specified.
   */
  public String getRebalanceStrategy() {
    return _record.getSimpleField(IdealStateProperty.REBALANCE_STRATEGY.name());
  }

  /**
   * Set the resource group name
   * @param resourceGroupName
   */
  public void setResourceGroupName(String resourceGroupName) {
    _record.setSimpleField(IdealStateProperty.RESOURCE_GROUP_NAME.toString(), resourceGroupName);
  }

  /**
   * Set the resource type
   * @param resourceType
   */
  public void setResourceType(String resourceType) {
    _record.setSimpleField(IdealStateProperty.RESOURCE_TYPE.toString(), resourceType);
  }

  /**
   * Get the resource type
   * @return the resource type, or null if none is being set
   */
  public String getResourceType() {
    return _record.getSimpleField(IdealStateProperty.RESOURCE_TYPE.toString());
  }

  /**
   * Set the delay time (in ms) that Helix should move the partition after an instance goes offline.
   * @param delayInMilliseconds
   */
  public void setRebalanceDelay(long delayInMilliseconds) {
    _record.setLongField(IdealStateProperty.REBALANCE_DELAY.name(), delayInMilliseconds);
  }

  /**
   * Get rebalance delay time (in ms).
   * @return
   */
  public long getRebalanceDelay() {
    return _record.getLongField(IdealStateProperty.REBALANCE_DELAY.name(), -1);
  }

  /**
   * Enable/Disable the delayed rebalance.
   * By default it is enabled if not set.
   *
   * @param enabled
   */
  public void setDelayRebalanceEnabled(boolean enabled) {
    _record.setBooleanField(ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name(), enabled);
  }

  /**
   * Whether the delay rebalance is enabled.
   * @return
   */
  public boolean isDelayRebalanceEnabled() {
    boolean disabled =
        _record.getBooleanField(IdealStateProperty.DELAY_REBALANCE_DISABLED.name(), false);
    boolean enabled =
        _record.getBooleanField(ResourceConfigProperty.DELAY_REBALANCE_ENABLED.name(), true);
    if (disabled) {
      return false;
    }
    return enabled;
  }

  /**
   * Get the resource group name
   *
   * @return
   */
  public String getResourceGroupName() {
    return _record.getSimpleField(IdealStateProperty.RESOURCE_GROUP_NAME.toString());
  }

  /**
   * Get if the resource group routing feature is enabled or not
   * By default, it's disabled
   *
   * @return true if enabled; false otherwise
   */
  public boolean isResourceGroupEnabled() {
    return _record.getBooleanField(IdealStateProperty.GROUP_ROUTING_ENABLED.name(), false);
  }

  /**
   * Enable/Disable the aggregated routing on resource group.
   *
   * @param enabled
   */
  public void enableGroupRouting(boolean enabled) {
    _record.setSimpleField(IdealStateProperty.GROUP_ROUTING_ENABLED.name(),
        Boolean.toString(enabled));
  }

  /**
   * If the external view for this resource is disabled. by default, it is false.
   *
   * @return true if the external view should be disabled for this resource.
   */
  public boolean isExternalViewDisabled() {
    return _record.getBooleanField(IdealStateProperty.EXTERNAL_VIEW_DISABLED.name(), false);
  }

  /**
   * Disable (true) or enable (false) External View for this resource.
   */
  public void setDisableExternalView(boolean disableExternalView) {
    _record
        .setSimpleField(IdealStateProperty.EXTERNAL_VIEW_DISABLED.name(),
            Boolean.toString(disableExternalView));
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
   * Get all of the partitions
   * @return a set of partition names
   */
  public Set<String> getPartitionSet() {
    if (getRebalanceMode() == RebalanceMode.SEMI_AUTO
        || getRebalanceMode() == RebalanceMode.FULL_AUTO
        || getRebalanceMode() == RebalanceMode.USER_DEFINED
        || getRebalanceMode() == RebalanceMode.TASK) {
      return _record.getListFields().keySet();
    } else if (getRebalanceMode() == RebalanceMode.CUSTOMIZED) {
      return _record.getMapFields().keySet();
    } else {
      logger.error("Invalid ideal state mode:" + getResourceName());
      return Collections.emptySet();
    }
  }

  /**
   * Get the current mapping of a partition.
   *
   * CAUTION: In FULL-AUTO mode, this method
   * could return empty map if neither {@link ClusterConfig#setPersistBestPossibleAssignment(Boolean)}
   * nor {@link ClusterConfig#setPersistIntermediateAssignment(Boolean)} is set to true.
   *
   * @param partitionName the name of the partition
   * @return the instances where the replicas live and the state of each
   */
  public Map<String, String> getInstanceStateMap(String partitionName) {
    return _record.getMapField(partitionName);
  }

  /**
   * Set the current mapping of a partition
   *
   * @param partitionName    the name of the partition
   * @param instanceStateMap instance->state mapping for this partition.
   * @return the instances where the replicas live and the state of each
   */
  public void setInstanceStateMap(String partitionName, Map<String, String> instanceStateMap) {
    _record.setMapField(partitionName, instanceStateMap);
  }

  /**
   * Get the instances who host replicas of a partition.
   * CAUTION: In FULL-AUTO mode, this method
   * could return empty set if neither {@link ClusterConfig#setPersistBestPossibleAssignment(Boolean)}
   * nor {@link ClusterConfig#setPersistIntermediateAssignment(Boolean)} is set to true.
   *
   * @return set of instance names
   */
  public Set<String> getInstanceSet(String partitionName) {
    switch(getRebalanceMode()) {
    case FULL_AUTO:
    case SEMI_AUTO:
    case USER_DEFINED:
    case TASK:
      List<String> prefList = _record.getListField(partitionName);
      if (prefList != null && !prefList.isEmpty()) {
        return new TreeSet<String>(prefList);
      } else {
        Map<String, String> stateMap = _record.getMapField(partitionName);
        if (stateMap != null && !stateMap.isEmpty()) {
          return new TreeSet<String>(stateMap.keySet());
        } else {
          logger.warn(partitionName + " does NOT exist");
        }
      }
      break;

    case CUSTOMIZED:
      Map<String, String> stateMap = _record.getMapField(partitionName);
      if (stateMap != null) {
        return new TreeSet<String>(stateMap.keySet());
      } else {
        logger.warn(partitionName + " does NOT exist");
      }
      break;
    case NONE:
    default:
      logger.warn("Invalid ideal state mode: " + getResourceName());
      break;
    }

    return Collections.emptySet();
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

    return null;
  }

  /**
   * Set the preference list of a partition
   *
   * @param partitionName the name of the partition
   * @param instanceList the instance preference list
   */
  public void setPreferenceList(String partitionName, List<String> instanceList) {
    _record.setListField(partitionName, instanceList);
  }

  /**
   * Set the preference lists for all partitions in this resource.
   *
   * @param instanceLists the map of instance preference lists.
   */
  public void setPreferenceLists(Map<String, List<String>> instanceLists) {
    _record.setListFields(instanceLists);
  }

  /**
   * Get the preference lists for all partitions
   *
   * @return map of lists of instances for all partitions in this resource.
   */
  public Map<String, List<String>> getPreferenceLists() {
    return _record.getListFields();
  }

  /**
   * Get the state model associated with this resource
   * @return an identifier of the state model
   */
  public String getStateModelDefRef() {
    return _record.getSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString());
  }

  /**
   * Set the state model associated with this resource
   * @param stateModel state model identifier
   */
  public void setStateModelDefRef(String stateModel) {
    _record.setSimpleField(IdealStateProperty.STATE_MODEL_DEF_REF.toString(), stateModel);
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
   * Set the number of minimal active partitions for this resource.
   *
   * @param minActiveReplicas
   */
  public void setMinActiveReplicas(int minActiveReplicas) {
    _record.setIntField(IdealStateProperty.MIN_ACTIVE_REPLICAS.toString(), minActiveReplicas);
  }

  /**
   * Get the number of minimal active partitions for this resource.
   *
   * @return
   */
  public int getMinActiveReplicas() {
    return _record.getIntField(IdealStateProperty.MIN_ACTIVE_REPLICAS.toString(), -1);
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
    // TODO: replica could be "ANY_INSTANCE".
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
        logger.error("could NOT determine replicas. set to 0");
        break;
      }
    }

    return replica;
  }

  /**
   * Get the number of replicas for each partition of this resource
   *
   * @return number of replicas
   */
  public int getReplicaCount(int eligibleInstancesCount) {
    String replicaStr = getReplicas();
    int replica = 0;

    try {
      replica = Integer.parseInt(replicaStr);
    } catch (NumberFormatException ex) {
      if (replicaStr
          .equalsIgnoreCase(ResourceConfig.ResourceConfigConstants.ANY_LIVEINSTANCE.name())) {
        replica = eligibleInstancesCount;
      } else {
        logger.error("Can not determine the replica count for resource " + getResourceName()
                + ", set to 0.");
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
   * Get the state model factory associated with this resource
   * @return state model factory name
   */
  public String getStateModelFactoryName() {
    return _record.getStringField(IdealStateProperty.STATE_MODEL_FACTORY_NAME.toString(),
        HelixConstants.DEFAULT_STATE_MODEL_FACTORY);
  }

  /**
   * Set the frequency with which to rebalance
   * @return the rebalancing timer period
   */
  public long getRebalanceTimerPeriod() {
    return _record.getLongField(IdealStateProperty.REBALANCE_TIMER_PERIOD.toString(), -1);
  }

  @Override
  public boolean isValid() {
    if (getNumPartitions() < 0) {
      logger.error("idealState:" + _record.getId() + " does not have number of partitions (was "
          + getNumPartitions() + ").");
      return false;
    }

    if (getStateModelDefRef() == null) {
      logger.error("idealStates:" + _record.getId() + " does not have state model definition.");
      return false;
    }

    if (getRebalanceMode() == RebalanceMode.SEMI_AUTO) {
      String replicaStr = getReplicas();
      if (replicaStr == null) {
        logger.error("invalid ideal-state. missing replicas in auto mode. record was: " + _record.getId());
        return false;
      }

      if (!replicaStr.equals(IdealStateConstants.ANY_LIVEINSTANCE.toString())) {
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
                    + ", record was: " + _record.getId());
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
      if (rebalancerName != null) {
        if (rebalancerName.equals(JobRebalancer.class.getName())
            || rebalancerName.equals(WorkflowRebalancer.class.getName())) {
          property = RebalanceMode.TASK;
        } else {
          if (LEGACY_TASK_REBALANCERS.contains(rebalancerName)) {
            // Print a warning message if legacy TASK rebalancer is used
            // Since legacy rebalancers have been removed, it is not safe just running legacy jobs and jobs/workflows
            //with current task assignment strategy.
            logger.warn("The rebalancer {} is not supported anymore. Setting rebalance mode to USER_DEFINED.",
                rebalancerName);
          }
          property = RebalanceMode.USER_DEFINED;
        }
      } else {
        property = RebalanceMode.SEMI_AUTO;
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
   * Parse a RebalanceMode from a string. It can also understand IdealStateModeProperty values.
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
        logger.error(e.toString());
      }
    }
    return rebalanceMode;
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
