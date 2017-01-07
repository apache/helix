package org.apache.helix.controller.stages;

import java.util.Set;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.model.ClusterConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

/**
 * Output for IntermediateStateCalStage.
 */
class StateTransitionThrottleController {
  private static Logger logger = Logger.getLogger(StateTransitionThrottleController.class);

  // pending allowed transition counts in the cluster level for recovery and load balance
  Map<StateTransitionThrottleConfig.RebalanceType, Long> _pendingTransitionAllowedInCluster;

  // pending allowed transition counts for each instance and resource
  Map<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>>
      _pendingTransitionAllowedPerInstance;
  Map<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>>
      _pendingTransitionAllowedPerResource;

  private boolean _throttleEnabled = false;

  public StateTransitionThrottleController(Set<String> resources, ClusterConfig clusterConfig,
      Set<String> liveInstances) {
    super();
    _pendingTransitionAllowedInCluster =
        new HashMap<StateTransitionThrottleConfig.RebalanceType, Long>();
    _pendingTransitionAllowedPerInstance =
        new HashMap<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>>();
    _pendingTransitionAllowedPerResource =
        new HashMap<String, Map<StateTransitionThrottleConfig.RebalanceType, Long>>();

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
        _pendingTransitionAllowedInCluster
            .put(config.getRebalanceType(), config.getMaxPartitionInTransition());
        _throttleEnabled = true;
        break;
      case RESOURCE:
        for (String resource : resources) {
          if (!_pendingTransitionAllowedPerResource.containsKey(resource)) {
            _pendingTransitionAllowedPerResource
                .put(resource, new HashMap<StateTransitionThrottleConfig.RebalanceType, Long>());
          }
          _pendingTransitionAllowedPerResource.get(resource)
              .put(config.getRebalanceType(), config.getMaxPartitionInTransition());
        }
        _throttleEnabled = true;
        break;
      case INSTANCE:
        for (String instance : liveInstances) {
          if (!_pendingTransitionAllowedPerInstance.containsKey(instance)) {
            _pendingTransitionAllowedPerInstance
                .put(instance, new HashMap<StateTransitionThrottleConfig.RebalanceType, Long>());
          }
          _pendingTransitionAllowedPerInstance.get(instance)
              .put(config.getRebalanceType(), config.getMaxPartitionInTransition());
        }
        _throttleEnabled = true;
        break;
      }
    }
  }

  /**
   * Whether any throttle config enabled for this cluster.
   *
   * @return
   */
  protected boolean isThrottleEnabled() {
    return _throttleEnabled;
  }

  /**
   * Check if state transition on a partition should be throttled.
   *
   * @return true if it should be throttled, otherwise, false.
   */
  protected boolean throttleforCluster(
      StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    Long clusterThrottle = _pendingTransitionAllowedInCluster.get(rebalanceType);
    if (clusterThrottle != null) {
      if (clusterThrottle <= 0) {
        return true;
      }
    }

    return false;
  }

  protected boolean throttleforResource(
      StateTransitionThrottleConfig.RebalanceType rebalanceType, String resourceName) {
    if (throttleforCluster(rebalanceType)) {
      return true;
    }

    Long resouceThrottle;
    if (_pendingTransitionAllowedPerResource.containsKey(resourceName)) {
      resouceThrottle = _pendingTransitionAllowedPerResource.get(resourceName).get(rebalanceType);
      if (resouceThrottle != null && resouceThrottle <= 0) {
        return true;
      }
    }

    return false;
  }

  protected boolean throttleForInstance(
      StateTransitionThrottleConfig.RebalanceType rebalanceType, String instanceName) {
    if (throttleforCluster(rebalanceType)) {
      return true;
    }

    Long instanceThrottle;
    if (_pendingTransitionAllowedPerInstance.containsKey(instanceName)) {
      instanceThrottle = _pendingTransitionAllowedPerInstance.get(instanceName).get(rebalanceType);
      if (instanceThrottle != null && instanceThrottle <= 0) {
        return true;
      }
    }

    return false;
  }

  protected void chargeCluster(StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    if (_pendingTransitionAllowedInCluster.containsKey(rebalanceType)) {
      Long clusterThrottle = _pendingTransitionAllowedInCluster.get(rebalanceType);
      if (clusterThrottle > 0) {
        _pendingTransitionAllowedInCluster.put(rebalanceType, clusterThrottle - 1);
      }
    }
  }

  protected void chargeResource(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      String resource) {
    if (_pendingTransitionAllowedPerResource.containsKey(resource)
        && _pendingTransitionAllowedPerResource.get(resource).containsKey(rebalanceType)) {
      Long resouceThrottle = _pendingTransitionAllowedPerResource.get(resource).get(rebalanceType);
      if (resouceThrottle > 0) {
        _pendingTransitionAllowedPerResource.get(resource).put(rebalanceType, resouceThrottle - 1);
      }
    }
  }

  protected void chargeInstance(StateTransitionThrottleConfig.RebalanceType rebalanceType,
      String instance) {
    if (_pendingTransitionAllowedPerInstance.containsKey(instance)
        && _pendingTransitionAllowedPerInstance.get(instance).containsKey(rebalanceType)) {
      Long instanceThrottle = _pendingTransitionAllowedPerInstance.get(instance).get(rebalanceType);
      if (instanceThrottle > 0) {
        _pendingTransitionAllowedPerInstance.get(instance).put(rebalanceType, instanceThrottle - 1);
      }
    }
  }
}

