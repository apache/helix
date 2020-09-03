package org.apache.helix.controller.changedetector.trimmer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ClusterConfig.ClusterConfigProperty;

/**
 * A singleton HelixProperty Trimmer for ClusterConfig to remove the non-cluster-topology-related
 * fields.
 */
public class ClusterConfigTrimmer extends HelixPropertyTrimmer<ClusterConfig> {
  private static final ClusterConfigTrimmer _clusterConfigTrimmer = new ClusterConfigTrimmer();

  /**
   * The following fields are considered as non-topology related.
   * HELIX_DISABLE_PIPELINE_TRIGGERS,
   * PERSIST_BEST_POSSIBLE_ASSIGNMENT,
   * PERSIST_INTERMEDIATE_ASSIGNMENT,
   * STATE_TRANSITION_THROTTLE_CONFIGS,
   * STATE_TRANSITION_CANCELLATION_ENABLED,
   * MISS_TOP_STATE_DURATION_THRESHOLD,
   * RESOURCE_PRIORITY_FIELD,
   * REBALANCE_TIMER_PERIOD,
   * MAX_CONCURRENT_TASK_PER_INSTANCE,
   * TARGET_EXTERNALVIEW_ENABLED,
   * DISABLED_INSTANCES,
   * QUOTA_TYPES,
   * MAX_OFFLINE_INSTANCES_ALLOWED,
   * NUM_OFFLINE_INSTANCES_FOR_AUTO_EXIT,
   * ERROR_PARTITION_THRESHOLD_FOR_LOAD_BALANCE,
   * ERROR_OR_RECOVERY_PARTITION_THRESHOLD_FOR_LOAD_BALANCE,
   * DELAY_REBALANCE_DISABLED,
   * DELAY_REBALANCE_ENABLED,
   * DELAY_REBALANCE_TIME,
   * GLOBAL_REBALANCE_ASYNC_MODE,
   * P2P_MESSAGE_ENABLED,
   * All StateTransitionTimeoutConfig,
   * All StateTransitionThrottleConfig
   */
  private static final Map<FieldType, Set<String>> STATIC_TOPOLOGY_RELATED_FIELD_MAP = ImmutableMap
      .of(FieldType.SIMPLE_FIELD, ImmutableSet
              .of(ClusterConfigProperty.TOPOLOGY.name(),
                  ClusterConfigProperty.FAULT_ZONE_TYPE.name(),
                  ClusterConfigProperty.TOPOLOGY_AWARE_ENABLED.name(),
                  ClusterConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name()),
          FieldType.LIST_FIELD, ImmutableSet
              .of(ClusterConfigProperty.INSTANCE_CAPACITY_KEYS.name()),
          FieldType.MAP_FIELD, ImmutableSet
              .of(ClusterConfigProperty.DEFAULT_INSTANCE_CAPACITY_MAP.name(),
              ClusterConfigProperty.DEFAULT_PARTITION_WEIGHT_MAP.name(),
              ClusterConfigProperty.REBALANCE_PREFERENCE.name()));

  private ClusterConfigTrimmer() {
  }

  @Override
  protected Map<FieldType, Set<String>> getNonTrimmableFields(ClusterConfig property) {
    return STATIC_TOPOLOGY_RELATED_FIELD_MAP;
  }

  @Override
  public ClusterConfig trimProperty(ClusterConfig property) {
    return new ClusterConfig(doTrim(property));
  }

  public static ClusterConfigTrimmer getInstance() {
    return _clusterConfigTrimmer;
  }
}
