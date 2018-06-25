package org.apache.helix.controller.stages;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * StateTransitionThrottleController is used to compute IntermediateState; it caches allowed
 * transition counts to see if any state transitions depending on the rebalance type must be held
 * off.
 */
class StateTransitionThrottleController {
  private static Logger logger = LoggerFactory.getLogger(StateTransitionThrottleController.class);

  // pending allowed transition counts in the cluster level for recovery and load balance
  private Map<StateTransitionThrottleConfig.RebalanceType, Long> _pendingTransitionAllowedInCluster;

  // pending allowed transition counts for each instance and resource
  private Map<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>> _pendingTransitionAllowedPerInstance;
  private Map<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>> _pendingTransitionAllowedPerResource;

  private boolean _throttleEnabled = false;

  public StateTransitionThrottleController(Set<String> resources, ClusterConfig clusterConfig,
      Set<String> liveInstances) {
    super();
    _pendingTransitionAllowedInCluster = new HashMap<>();
    _pendingTransitionAllowedPerInstance = new HashMap<>();
    _pendingTransitionAllowedPerResource = new HashMap<>();

    if (clusterConfig == null) {
      logger.warn("Cluster config is not found, no throttle config set!");
      return;
    }

    List<StateTransitionThrottleConfig> throttleConfigs =
        clusterConfig.getStateTransitionThrottleConfigs();

    if (throttleConfigs == null || throttleConfigs.isEmpty()) {
      logger.info("No throttle config is set!");
      return;
    }

    for (StateTransitionThrottleConfig config : throttleConfigs) {
      switch (config.getThrottleScope()) {
        case CLUSTER:
          _pendingTransitionAllowedInCluster.put(config.getRebalanceType(),
              config.getMaxPartitionInTransition());
          _throttleEnabled = true;
          break;
        case RESOURCE:
          for (String resource : resources) {
            if (!_pendingTransitionAllowedPerResource.containsKey(resource)) {
              _pendingTransitionAllowedPerResource.put(resource,
                  new HashMap<StateTransitionThrottleConfig.RebalanceType, Long>());
            }
            _pendingTransitionAllowedPerResource.get(resource).put(config.getRebalanceType(),
                config.getMaxPartitionInTransition());
          }
          _throttleEnabled = true;
          break;
        case INSTANCE:
          for (String instance : liveInstances) {
            if (!_pendingTransitionAllowedPerInstance.containsKey(instance)) {
              _pendingTransitionAllowedPerInstance.put(instance,
                  new HashMap<StateTransitionThrottleConfig.RebalanceType, Long>());
            }
            _pendingTransitionAllowedPerInstance.get(instance).put(config.getRebalanceType(),
                config.getMaxPartitionInTransition());
          }
          _throttleEnabled = true;
          break;
      }
    }
  }

  /**
   * Returns the flag that indicates throttling is applied at any level (cluster, resource, and
   * instance).
   * @return true if throttling operation is present
   */
  protected boolean isThrottleEnabled() {
    return _throttleEnabled;
  }

  /**
   * Check if state transitions for a particular Rebalance type must be throttled. Assuming the
   * "charging" already happened at this level, this method purely checks whether the throttle value
   * has reached 0 or below.
   * @return true if it should be throttled, otherwise, false
   */
  protected boolean shouldThrottleForCluster(
      StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    if (shouldThrottleForANYType(_pendingTransitionAllowedInCluster)) {
      return true;
    }
    Long clusterThrottle = _pendingTransitionAllowedInCluster.get(rebalanceType);
    return clusterThrottle != null && clusterThrottle <= 0;
  }

  /**
   * Check if state transitions for a particular Rebalance type must be throttled at the resource
   * level. Assuming the "charging" already happened at this level and the throttle value did not
   * dip below 0 at the higher level, this method purely checks whether the throttle value has
   * reached 0 or below.
   * @return true if it should be throttled, otherwise, false
   */
  protected boolean shouldThrottleForResource(
      StateTransitionThrottleConfig.RebalanceType rebalanceType, String resourceName) {
    if (shouldThrottleForCluster(rebalanceType)) {
      return true;
    }
    Long resourceThrottle;
    if (_pendingTransitionAllowedPerResource.containsKey(resourceName)) {
      resourceThrottle = _pendingTransitionAllowedPerResource.get(resourceName).get(rebalanceType);
      if (shouldThrottleForANYType(_pendingTransitionAllowedPerResource.get(resourceName))
          || (resourceThrottle != null && resourceThrottle <= 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check if state transitions for a particular Rebalance type must be throttled at the resource
   * level. Assuming the "charging" already happened at this level and the throttle value did not
   * dip below 0 at the higher level, this method purely checks whether the throttle value has
   * reached 0 or below.
   * @return true if it should be throttled, otherwise, false
   */
  protected boolean shouldThrottleForInstance(
      StateTransitionThrottleConfig.RebalanceType rebalanceType, String instanceName) {
    if (shouldThrottleForCluster(rebalanceType)) {
      return true;
    }
    Long instanceThrottle;
    if (_pendingTransitionAllowedPerInstance.containsKey(instanceName)) {
      instanceThrottle = _pendingTransitionAllowedPerInstance.get(instanceName).get(rebalanceType);
      if (shouldThrottleForANYType(_pendingTransitionAllowedPerInstance.get(instanceName))
          || (instanceThrottle != null && instanceThrottle <= 0)) {
        return true;
      }
    }
    return false;
  }

  /**
   * "Charge" for a pending state for a particular Rebalance type by subtracting one pending state
   * from number of total pending states allowed (set by user application).
   * @param rebalanceType
   */
  protected void chargeCluster(StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    if (_pendingTransitionAllowedInCluster.containsKey(rebalanceType)) {
      Long clusterThrottle = _pendingTransitionAllowedInCluster.get(rebalanceType);
      chargeANYType(_pendingTransitionAllowedInCluster);
      if (clusterThrottle > 0) {
        _pendingTransitionAllowedInCluster.put(rebalanceType, clusterThrottle - 1);
      }
    }
  }

  /**
   * "Charge" for a pending state for a particular Rebalance type by subtracting one pending state
   * from number of total pending states allowed (set by user application).
   * @param rebalanceType
   */
  protected void chargeResource(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      String resource) {
    if (_pendingTransitionAllowedPerResource.containsKey(resource)
        && _pendingTransitionAllowedPerResource.get(resource).containsKey(rebalanceType)) {
      chargeANYType(_pendingTransitionAllowedPerResource.get(resource));
      Long resourceThrottle = _pendingTransitionAllowedPerResource.get(resource).get(rebalanceType);
      if (resourceThrottle > 0) {
        _pendingTransitionAllowedPerResource.get(resource).put(rebalanceType, resourceThrottle - 1);
      }
    }
  }

  /**
   * "Charge" for a pending state for a particular Rebalance type by subtracting one pending state
   * from number of total pending states allowed (set by user application).
   * @param rebalanceType
   */
  protected void chargeInstance(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      String instance) {
    if (_pendingTransitionAllowedPerInstance.containsKey(instance)
        && _pendingTransitionAllowedPerInstance.get(instance).containsKey(rebalanceType)) {
      chargeANYType(_pendingTransitionAllowedPerInstance.get(instance));
      Long instanceThrottle = _pendingTransitionAllowedPerInstance.get(instance).get(rebalanceType);
      if (instanceThrottle > 0) {
        _pendingTransitionAllowedPerInstance.get(instance).put(rebalanceType, instanceThrottle - 1);
      }
    }
  }

  /**
   * Check if state transitions must be throttled overall regardless of the rebalance type.
   * Assuming the "charging" already happened, this method purely checks whether the throttle value
   * has reached 0 or below.
   * @param pendingTransitionAllowed
   * @return true if it should be throttled, otherwise, false
   */
  private boolean shouldThrottleForANYType(
      Map<StateTransitionThrottleConfig.RebalanceType, Long> pendingTransitionAllowed) {
    if (pendingTransitionAllowed.containsKey(StateTransitionThrottleConfig.RebalanceType.ANY)) {
      Long anyTypeThrottle =
          pendingTransitionAllowed.get(StateTransitionThrottleConfig.RebalanceType.ANY);
      if (anyTypeThrottle != null && anyTypeThrottle <= 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * "Charge" for a pending state regardless of the rebalance type by subtracting one pending state
   * from number of total pending state from number of total pending states allowed (set by user
   * application).
   * @param pendingTransitionAllowed
   */
  private void chargeANYType(
      Map<StateTransitionThrottleConfig.RebalanceType, Long> pendingTransitionAllowed) {
    if (pendingTransitionAllowed.containsKey(StateTransitionThrottleConfig.RebalanceType.ANY)) {
      Long anyTypeThrottle =
          pendingTransitionAllowed.get(StateTransitionThrottleConfig.RebalanceType.ANY);
      if (anyTypeThrottle > 0) {
        pendingTransitionAllowed.put(StateTransitionThrottleConfig.RebalanceType.ANY,
            anyTypeThrottle - 1);
      }
    }
  }
}