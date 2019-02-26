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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.HelixConfigProperty;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;

/**
 * Cluster configurations
 */
public class ClusterConfig extends HelixProperty {
  /**
   * Configurable characteristics of a cluster.
   * NOTE: Do NOT use this field name directly, use its corresponding getter/setter in the
   * ClusterConfig.
   */
  public enum ClusterConfigProperty {
    HELIX_DISABLE_PIPELINE_TRIGGERS,
    PERSIST_BEST_POSSIBLE_ASSIGNMENT,
    PERSIST_INTERMEDIATE_ASSIGNMENT,
    TOPOLOGY, // cluster topology definition, for example, "/zone/rack/host/instance"
    FAULT_ZONE_TYPE, // the type in which isolation should be applied on when Helix places the
    // replicas from same partition.
    TOPOLOGY_AWARE_ENABLED, // whether topology aware rebalance is enabled.
    @Deprecated
    DELAY_REBALANCE_DISABLED, // disabled the delayed rebalaning in case node goes offline.
    DELAY_REBALANCE_ENABLED, // whether the delayed rebalaning is enabled.
    DELAY_REBALANCE_TIME, // delayed time in ms that the delay time Helix should hold until
    // rebalancing.
    STATE_TRANSITION_THROTTLE_CONFIGS,
    STATE_TRANSITION_CANCELLATION_ENABLED,
    MISS_TOP_STATE_DURATION_THRESHOLD,
    RESOURCE_PRIORITY_FIELD,
    REBALANCE_TIMER_PERIOD,
    MAX_CONCURRENT_TASK_PER_INSTANCE,

    // The following concerns maintenance mode
    MAX_PARTITIONS_PER_INSTANCE,
    // The following two include offline AND disabled instances
    MAX_OFFLINE_INSTANCES_ALLOWED,
    NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT, // For auto-exiting maintenance mode

    TARGET_EXTERNALVIEW_ENABLED,
    @Deprecated // ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE will take
        // precedence if it is set
        ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE, // Controller won't execute load balance state
    // transition if the number of partitons that need
    // recovery exceeds this limitation
    ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE, // Controller won't execute load balance
    // state transition if the number of
    // partitons that need recovery or in
    // error exceeds this limitation
    DISABLED_INSTANCES,
    VIEW_CLUSTER, // Set to "true" to indicate this is a view cluster
    VIEW_CLUSTER_SOURCES, // Map field, key is the name of source cluster, value is
    // ViewClusterSourceConfig JSON string
    VIEW_CLUSTER_REFRESH_PERIOD, // In second

    // Specifies job types and used for quota allocation
    QUOTA_TYPES
  }

  private final static int DEFAULT_MAX_CONCURRENT_TASK_PER_INSTANCE = 40;
  // By default, no load balance if any error partition
  @Deprecated
  private final static int DEFAULT_ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE = 0;
  // By default, no load balance if any error or recovery partition. -1 implies that the threshold
  // is not set and will be given a default value of 1
  private final static int DEFAULT_ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE = -1;
  private static final String IDEAL_STATE_RULE_PREFIX = "IdealStateRule!";
  private final static int DEFAULT_VIEW_CLUSTER_REFRESH_PERIOD = 30;

  public final static String TASK_QUOTA_RATIO_NOT_SET = "-1";

  /**
   * Instantiate for a specific cluster
   * @param cluster the cluster identifier
   */
  public ClusterConfig(String cluster) {
    super(cluster);
  }

  /**
   * Instantiate with a pre-populated record
   *
   * @param record a ZNRecord corresponding to a cluster configuration
   */
  public ClusterConfig(ZNRecord record) {
    super(record);
  }

  public void setViewCluster() {
    _record.setBooleanField(ClusterConfigProperty.VIEW_CLUSTER.name(), true);
  }

  /**
   * Whether this cluster is a ViewCluster
   * @return
   */
  public boolean isViewCluster() {
    return _record
        .getBooleanField(ClusterConfigProperty.VIEW_CLUSTER.name(), false);
  }

  /**
   * Set task quota type with the ratio of this quota.
   * @param quotaType String
   * @param quotaRatio int
   */
  public void setTaskQuotaRatio(String quotaType, int quotaRatio) {
    if (_record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()) == null) {
      _record.setMapField(ClusterConfigProperty.QUOTA_TYPES.name(), new HashMap<String, String>());
    }
    _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name())
        .put(quotaType, Integer.toString(quotaRatio));
  }

  /**
   * Set task quota type with the ratio of this quota. Quota ratio must be a String that is
   * parse-able into an int.
   * @param quotaType String
   * @param quotaRatio String
   */
  public void setTaskQuotaRatio(String quotaType, String quotaRatio) {
    if (_record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()) == null) {
      _record.setMapField(ClusterConfigProperty.QUOTA_TYPES.name(), new HashMap<String, String>());
    }
    _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name())
        .put(quotaType, quotaRatio);
  }

  /**
   * Remove task quota with the given quota type.
   * @param quotaType
   */
  public void removeTaskQuotaRatio(String quotaType) {
    if (_record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()) != null) {
      _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()).remove(quotaType);
    }
  }

  /**
   * Given quota type, return ratio of the quota. If quota type does not exist, return "0"
   * @param quotaType quota type
   * @return ratio of quota type
   */
  public String getTaskQuotaRatio(String quotaType) {
    if (_record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()) == null
        || _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()).get(quotaType) == null) {
      return TASK_QUOTA_RATIO_NOT_SET;
    }

    return _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()).get(quotaType);
  }

  /**
   * Get all task quota and their ratios
   *
   * @return a task quota -> quota ratio mapping
   */
  public Map<String, String> getTaskQuotaRatioMap() {
    return _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name());
  }

  /**
   * Resets all quota-related information in this ClusterConfig.
   */
  public void resetTaskQuotaRatioMap() {
    if (_record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()) != null) {
      _record.getMapField(ClusterConfigProperty.QUOTA_TYPES.name()).clear();
    }
  }

  /**
   * Set view cluster max refresh period
   * @param refreshPeriod refresh period in second
   */
  public void setViewClusterRefreshPeriod(int refreshPeriod) {
    _record.setIntField(ClusterConfigProperty.VIEW_CLUSTER_REFRESH_PERIOD.name(),
        refreshPeriod);
  }

  public int getViewClusterRefershPeriod() {
    return _record.getIntField(ClusterConfigProperty.VIEW_CLUSTER_REFRESH_PERIOD.name(),
        DEFAULT_VIEW_CLUSTER_REFRESH_PERIOD);
  }

  /**
   * Whether to persist best possible assignment in a resource's idealstate.
   *
   * @return
   */
  public Boolean isPersistBestPossibleAssignment() {
    return _record
        .getBooleanField(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString(), false);
  }

  /**
   * Enable/Disable persist best possible assignment in a resource's idealstate.
   * CAUTION: if both {@link #setPersistBestPossibleAssignment(Boolean)} and {@link #setPersistIntermediateAssignment(Boolean)}
   * are set to true, the IntermediateAssignment will be persisted into IdealState's map field.
   * By default, it is DISABLED if not set.
   * @return
   */
  public void setPersistBestPossibleAssignment(Boolean enable) {
    if (enable == null) {
      _record.getSimpleFields().remove(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString());
    } else {
      _record.setBooleanField(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString(), enable);
    }
  }

  /**
   * Whether to persist IntermediateAssignment in a resource's idealstate.
   *
   * @return
   */
  public Boolean isPersistIntermediateAssignment() {
    return _record
        .getBooleanField(ClusterConfigProperty.PERSIST_INTERMEDIATE_ASSIGNMENT.toString(), false);
  }

  /**
   * Enable/Disable persist IntermediateAssignment in a resource's idealstate.
   * CAUTION: if both {@link #setPersistBestPossibleAssignment(Boolean)} and {@link #setPersistIntermediateAssignment(Boolean)}
   * are set to true, the IntermediateAssignment will be persisted into IdealState's map field.
   * By default, it is DISABLED if not set.
   * @return
   */
  public void setPersistIntermediateAssignment(Boolean enable) {
    if (enable == null) {
      _record.getSimpleFields().remove(ClusterConfigProperty.PERSIST_INTERMEDIATE_ASSIGNMENT.toString());
    } else {
      _record.setBooleanField(ClusterConfigProperty.PERSIST_INTERMEDIATE_ASSIGNMENT.toString(), enable);
    }
  }

  public Boolean isPipelineTriggersDisabled() {
    return _record
        .getBooleanField(ClusterConfigProperty.HELIX_DISABLE_PIPELINE_TRIGGERS.toString(), false);
  }

  /**
   * Enable/disable topology aware rebalacning. If enabled, both {@link #setTopology(String)} and
   * {@link #setFaultZoneType(String)} should be set.
   * By default, this is DISABLED if not set.
   *
   * @param enabled
   */
  public void setTopologyAwareEnabled(boolean enabled) {
    _record.setBooleanField(ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), enabled);
  }

  /**
   * Whether topology aware rebalance is enabled for this cluster.
   * By default, it is DISABLED.
   * @return
   */
  public boolean isTopologyAwareEnabled() {
    return _record.getBooleanField(ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(), false);
  }

  /**
   * Set cluster topology, this is used for topology-aware rebalancer.
   * @param topology
   */
  public void setTopology(String topology) {
    _record.setSimpleField(ClusterConfigProperty.TOPOLOGY.name(), topology);
  }

  /**
   * Get cluster topology.
   *
   * @return
   */
  public String getTopology() {
    return _record.getSimpleField(ClusterConfigProperty.TOPOLOGY.name());
  }

  /**
   * Set cluster fault zone type, this should be set combined with {@link #setTopology(String)}.
   * @param faultZoneType
   */
  public void setFaultZoneType(String faultZoneType) {
    _record.setSimpleField(ClusterConfigProperty.FAULT_ZONE_TYPE.name(), faultZoneType);
  }

  /**
   * Get cluster fault zone type.
   *
   * @return
   */
  public String getFaultZoneType() {
    return _record.getSimpleField(ClusterConfigProperty.FAULT_ZONE_TYPE.name());
  }

  /**
   * Set the delayed rebalance time, this applies only when {@link #isDelayRebalaceEnabled()} is
   * true.
   *
   * @param milliseconds
   */
  public void setRebalanceDelayTime(long milliseconds) {
    _record.setLongField(ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), milliseconds);
  }

  public long getRebalanceDelayTime() {
    return _record.getLongField(ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), -1);
  }

  /**
   * Disable/enable delay rebalance.
   * By default, this is ENABLED if not set.
   *
   * @param enabled
   */
  public void setDelayRebalaceEnabled(boolean enabled) {
    _record.setBooleanField(ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name(), enabled);
  }

  /**
   * Whether Delay rebalance is enabled for this cluster.
   *
   * @return
   */
  public boolean isDelayRebalaceEnabled() {
    boolean disabled =
        _record.getBooleanField(ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(), false);
    boolean enabled =
        _record.getBooleanField(ClusterConfigProperty.DELAY_REBALANCE_ENABLED.name(), true);
    if (disabled) {
      return false;
    }
    return enabled;
  }

  /**
   * Enable/Disable state transition cancellation for the cluster
   * @param enable
   */
  public void stateTransitionCancelEnabled(Boolean enable) {
    if (enable == null) {
      _record.getSimpleFields()
          .remove(ClusterConfigProperty.STATE_TRANSITION_CANCELLATION_ENABLED.name());
    } else {
      _record.setBooleanField(ClusterConfigProperty.STATE_TRANSITION_CANCELLATION_ENABLED.name(),
          enable);
    }
  }

  /**
   * Set the maximum number of partitions that an instance can serve in this cluster.
   *
   * @param maxPartitionsPerInstance the maximum number of partitions supported
   */
  public void setMaxPartitionsPerInstance(int maxPartitionsPerInstance) {
    _record.setIntField(ClusterConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
        maxPartitionsPerInstance);
  }

  /**
   * Get the maximum number of partitions an instance can serve in this cluster.
   *
   * @return the partition capacity of an instance for this resource, or Integer.MAX_VALUE
   */
  public int getMaxPartitionsPerInstance() {
    return _record.getIntField(ClusterConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(), -1);
  }

  /**
   * Set the max offline instances allowed for the cluster. If number of pff-line or disabled instances
   *  in the cluster reach this limit, Helix will pause the cluster.
   *
   * @param maxOfflineInstancesAllowed
   */
  public void setMaxOfflineInstancesAllowed(int maxOfflineInstancesAllowed) {
    _record.setIntField(ClusterConfigProperty.MAX_OFFLINE_INSTANCES_ALLOWED.name(),
        maxOfflineInstancesAllowed);
  }

  /**
   * Get the max offline instances allowed for the cluster.
   *
   * @return
   */
  public int getMaxOfflineInstancesAllowed() {
    return _record.getIntField(ClusterConfigProperty.MAX_OFFLINE_INSTANCES_ALLOWED.name(), -1);
  }

  /**
   * Sets Maintenance recovery threshold so that the cluster could auto-exit maintenance mode.
   * Values less than 0 will disable auto-exit.
   * @param maintenanceRecoveryThreshold
   */
  public void setMaintenanceRecoveryThreshold(int maintenanceRecoveryThreshold)
      throws HelixException {
    int maxOfflineInstancesAllowed = getMaxOfflineInstancesAllowed();
    if (maxOfflineInstancesAllowed >= 0) {
      // MaintenanceRecoveryThreshold must be more strict than maxOfflineInstancesAllowed
      if (maintenanceRecoveryThreshold > maxOfflineInstancesAllowed) {
        throw new HelixException(
            "Maintenance recovery threshold must be less than equal to maximum offline instances allowed!");
      }
    }
    _record.setIntField(ClusterConfigProperty.NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT.name(),
        maintenanceRecoveryThreshold);
  }

  /**
   * Returns Maintenance recovery threshold. In order for the cluster to auto-exit maintenance mode,
   * the number of offline/disabled instances must be less than or equal to this threshold.
   * -1 indicates that there will be no auto-exit.
   * @return
   */
  public int getMaintenanceRecoveryThreshold() {
    return _record.getIntField(ClusterConfigProperty.NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT.name(), -1);
  }

  /**
   * Set the resource prioritization field. It should be Integer field and sortable.
   *
   * IMPORTANT: The sorting order is DESCENDING order, which means the larger number will have
   * higher priority. If user did not set up the field in ResourceConfig or IdealState or the field
   * is not parseable, Helix will treat it as lowest priority.
   *
   * @param priorityField
   */
  public void setResourcePriorityField(String priorityField) {
    _record.setSimpleField(ClusterConfigProperty.RESOURCE_PRIORITY_FIELD.name(), priorityField);
  }

  public String getResourcePriorityField() {
    return _record.getSimpleField(ClusterConfigProperty.RESOURCE_PRIORITY_FIELD.name());
  }

  /**
   * Set the period that controller should sync up its local cache and perform a rebalance.
   * @param milliseconds
   */
  public void setRebalanceTimePeriod(long milliseconds) {
    _record.setLongField(ClusterConfigProperty.REBALANCE_TIMER_PERIOD.name(), milliseconds);
  }

  public long getRebalanceTimePeriod() {
    return _record.getLongField(ClusterConfigProperty.REBALANCE_TIMER_PERIOD.name(), -1);
  }

  public boolean isStateTransitionCancelEnabled() {
    return _record
        .getBooleanField(ClusterConfigProperty.STATE_TRANSITION_CANCELLATION_ENABLED.name(), false);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ClusterConfig) {
      ClusterConfig that = (ClusterConfig) obj;

      if (this.getId().equals(that.getId())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get a list StateTransitionThrottleConfig set for this cluster.
   *
   * @return
   */
  public List<StateTransitionThrottleConfig> getStateTransitionThrottleConfigs() {
    List<String> configs =
        _record.getListField(ClusterConfigProperty.STATE_TRANSITION_THROTTLE_CONFIGS.name());
    if (configs == null || configs.isEmpty()) {
      return Collections.emptyList();
    }
    List<StateTransitionThrottleConfig> throttleConfigs =
        new ArrayList<StateTransitionThrottleConfig>();
    for (String configstr : configs) {
      StateTransitionThrottleConfig throttleConfig =
          StateTransitionThrottleConfig.fromJSON(configstr);
      if (throttleConfig != null) {
        throttleConfigs.add(throttleConfig);
      }
    }

    return throttleConfigs;
  }

  /**
   * Set StateTransitionThrottleConfig for this cluster.
   *
   * @param throttleConfigs
   */
  public void setStateTransitionThrottleConfigs(
      List<StateTransitionThrottleConfig> throttleConfigs) {
    List<String> configStrs = new ArrayList<String>();

    for (StateTransitionThrottleConfig throttleConfig : throttleConfigs) {
      String configStr = throttleConfig.toJSON();
      if (configStr != null) {
        configStrs.add(configStr);
      }
    }

    if (!configStrs.isEmpty()) {
      _record
          .setListField(ClusterConfigProperty.STATE_TRANSITION_THROTTLE_CONFIGS.name(), configStrs);
    }
  }

  /**
   * Set the missing top state duration threshold
   *
   * If top-state hand off duration is greater than this threshold, Helix will count that handoff
   * as failed and report it with missingtopstate metrics. If this thresold is not set,
   * Long.MAX_VALUE will be used as the default value, which means no top-state hand-off will be
   * treated as failure.
   *
   * @param durationThreshold
   */
  public void setMissTopStateDurationThreshold(long durationThreshold) {
    _record.setLongField(ClusterConfigProperty.MISS_TOP_STATE_DURATION_THRESHOLD.name(),
        durationThreshold);
  }

  /**
   * Get the missing top state duration threshold
   * @return
   */
  public long getMissTopStateDurationThreshold() {
    return _record.getLongField(ClusterConfigProperty.MISS_TOP_STATE_DURATION_THRESHOLD.name(),
        Long.MAX_VALUE);
  }

  /**
   * Set cluster level state transition time out
   * @param stateTransitionTimeoutConfig
   */
  public void setStateTransitionTimeoutConfig(
      StateTransitionTimeoutConfig stateTransitionTimeoutConfig) {
    _record.setMapField(StateTransitionTimeoutConfig.StateTransitionTimeoutProperty.TIMEOUT.name(),
        stateTransitionTimeoutConfig.getTimeoutMap());
  }

  /**
   * Get the state transition timeout at cluster level
   * @return
   */
  public StateTransitionTimeoutConfig getStateTransitionTimeoutConfig() {
    return StateTransitionTimeoutConfig.fromRecord(_record);
  }

  /**
   * Enable/disable target externalview persist
   * @param enabled
   */
  public void enableTargetExternalView(boolean enabled) {
    _record.setBooleanField(ClusterConfigProperty.TARGET_EXTERNALVIEW_ENABLED.name(), enabled);
  }

  /**
   * Determine whether target externalview is enabled or disabled
   * @return
   */
  public boolean isTargetExternalViewEnabled() {
    return _record.getBooleanField(ClusterConfigProperty.TARGET_EXTERNALVIEW_ENABLED.name(), false);
  }

  /**
   * Get maximum allowed running task count on all instances in this cluster.
   * @return the maximum task count
   */
  public int getMaxConcurrentTaskPerInstance() {
    return _record.getIntField(ClusterConfigProperty.MAX_CONCURRENT_TASK_PER_INSTANCE.name(),
        DEFAULT_MAX_CONCURRENT_TASK_PER_INSTANCE);
  }

  /**
   * Set maximum allowed running task count on all instances in this cluster.
   * Instance level configuration will override cluster configuration.
   * @param maxConcurrentTaskPerInstance the maximum task count
   */
  public void setMaxConcurrentTaskPerInstance(int maxConcurrentTaskPerInstance) {
    _record.setIntField(ClusterConfigProperty.MAX_CONCURRENT_TASK_PER_INSTANCE.name(),
        maxConcurrentTaskPerInstance);
  }

  /**
   * Get maximum allowed error partitions for a resource to be load balanced.
   * If limitation is set to negative number, Helix won't check error partition count before
   * schedule load balance.
   * @return the maximum allowed error partition count
   */
  public int getErrorPartitionThresholdForLoadBalance() {
    return _record.getIntField(
        ClusterConfigProperty.ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE.name(),
        DEFAULT_ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE);
  }

  /**
   * Set maximum allowed error partitions for a resource to be load balanced.
   * If limitation is set to negative number, Helix won't check error partition count before
   * schedule load balance.
   * @param errorPartitionThreshold the maximum allowed error partition count
   */
  public void setErrorPartitionThresholdForLoadBalance(int errorPartitionThreshold) {
    _record.setIntField(ClusterConfigProperty.ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE.name(),
        errorPartitionThreshold);
  }

  /**
   * Get the threshold for the number of partitions needing recovery or in error. Default value is set at
   * Integer.MAX_VALUE to allow recovery rebalance and load rebalance to happen in the same pipeline
   * cycle. If the number of partitions needing recovery is greater than this threshold, recovery
   * balance will take precedence and load balance will not happen during this cycle.
   * @return the threshold
   */
  public int getErrorOrRecoveryPartitionThresholdForLoadBalance() {
    return _record.getIntField(
        ClusterConfigProperty.ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE.name(),
        DEFAULT_ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE);
  }

  /**
   * Set the threshold for the number of partitions needing recovery or in error. Default value is set at
   * Integer.MAX_VALUE to allow recovery rebalance and load rebalance to happen in the same pipeline
   * cycle. If the number of partitions needing recovery is greater than this threshold, recovery
   * balance will take precedence and load balance will not happen during this cycle.
   * @param recoveryPartitionThreshold
   */
  public void setErrorOrRecoveryPartitionThresholdForLoadBalance(int recoveryPartitionThreshold) {
    _record.setIntField(ClusterConfigProperty.ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE.name(),
        recoveryPartitionThreshold);
  }

  /**
   * Set the disabled instance list
   * @param disabledInstances
   */
  public void setDisabledInstances(Map<String, String> disabledInstances) {
    _record.setMapField(ClusterConfigProperty.DISABLED_INSTANCES.name(), disabledInstances);
  }

  /**
   * Get current disabled instance map of <instance, disabledTimeStamp>
   * @return
   */
  public Map<String, String> getDisabledInstances() {
    return _record.getMapField(ClusterConfigProperty.DISABLED_INSTANCES.name());
  }

  /**
   * Whether the P2P state transition message is enabled for all resources in this cluster. By
   * default it is disabled if not set.
   *
   * @return
   */
  public boolean isP2PMessageEnabled() {
    return _record.getBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), false);
  }

  /**
   * Enable P2P state transition message for all resources in this cluster. P2P State Transition
   * message can reduce the top-state replica unavailable time during top-state handoff period. This
   * only applies for those resources with state models that only have a single top-state replica,
   * such as MasterSlave or LeaderStandy models. By default P2P message is disabled if not set.
   *
   * @param enabled
   */
  public void enableP2PMessage(boolean enabled) {
    _record.setBooleanField(HelixConfigProperty.P2P_MESSAGE_ENABLED.name(), enabled);
  }

  /**
   * Get IdealState rules defined in the cluster config.
   *
   * @return
   */
  public Map<String, Map<String, String>> getIdealStateRules() {
    Map<String, Map<String, String>> idealStateRuleMap = new HashMap<>();

    for (String simpleKey : getRecord().getSimpleFields().keySet()) {
      if (simpleKey.startsWith(IDEAL_STATE_RULE_PREFIX)) {
        String simpleValue = getRecord().getSimpleField(simpleKey);
        String[] rules = simpleValue.split("(?<!\\\\),");
        Map<String, String> singleRule = Maps.newHashMap();
        for (String rule : rules) {
          String[] keyValue = rule.split("(?<!\\\\)=");
          if (keyValue.length >= 2) {
            singleRule.put(keyValue[0], keyValue[1]);
          }
        }
        idealStateRuleMap.put(simpleKey, singleRule);
      }
    }
    return idealStateRuleMap;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  /**
   * Get the name of this resource
   *
   * @return the instance name
   */
  public String getClusterName() {
    return _record.getId();
  }
}