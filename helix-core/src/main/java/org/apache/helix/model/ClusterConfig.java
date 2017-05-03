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
import java.util.List;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.api.config.StateTransitionTimeoutConfig;

/**
 * Cluster configurations
 */
public class ClusterConfig extends HelixProperty {
  /**
   * Configurable characteristics of a cluster.
   *
   * NOTE: Do NOT use this field name directly, use its corresponding getter/setter in the ClusterConfig.
   */
  public enum ClusterConfigProperty {
    HELIX_DISABLE_PIPELINE_TRIGGERS,
    PERSIST_BEST_POSSIBLE_ASSIGNMENT,
    PERSIST_INTERMEDIATE_ASSIGNMENT,
    TOPOLOGY,  // cluster topology definition, for example, "/zone/rack/host/instance"
    FAULT_ZONE_TYPE, // the type in which isolation should be applied on when Helix places the replicas from same partition.
    TOPOLOGY_AWARE_ENABLED, // whether topology aware rebalance is enabled.
    @Deprecated
    DELAY_REBALANCE_DISABLED,  // disabled the delayed rebalaning in case node goes offline.
    DELAY_REBALANCE_ENABLED,  // whether the delayed rebalaning is enabled.
    DELAY_REBALANCE_TIME,    // delayed time in ms that the delay time Helix should hold until rebalancing.
    STATE_TRANSITION_THROTTLE_CONFIGS,
    STATE_TRANSITION_CANCELLATION_ENABLED,
    MISS_TOP_STATE_DURATION_THRESHOLD,
    RESOURCE_PRIORITY_FIELD,
    REBALANCE_TIMER_PERIOD
  }

  /**
   * Instantiate for a specific cluster
   *
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

