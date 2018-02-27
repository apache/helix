package org.apache.helix.util;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.rebalancer.strategy.ConstraintRebalanceStrategy;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A rebalance tool that generate an resource partition assignment based on the input.
 * Note the assignment won't be automatically applied to the cluster. Applications are supposed to
 * apply the change.
 */
public class WeightAwareRebalanceUtil {
  private final ClusterConfig _clusterConfig;
  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  // For the possible customized state models.
  private final Map<String, StateModelDefinition> _stateModelDefs = new HashMap<>();
  private final ClusterDataCache _dataCache;

  public enum RebalanceOption {
    INIT,
    REASSIGN
  }

  public WeightAwareRebalanceUtil(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs) {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      _instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
    _clusterConfig = clusterConfig;

    _dataCache = new ClusterDataCache();
    _dataCache.setInstanceConfigMap(_instanceConfigMap);
    _dataCache.setClusterConfig(_clusterConfig);
    List<LiveInstance> liveInstanceList = new ArrayList<>();
    for (String instance : _instanceConfigMap.keySet()) {
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstanceList.add(liveInstance);
    }
    _dataCache.setLiveInstances(liveInstanceList);
  }

  /**
   * The method to generate partition assignment mappings.
   *
   * @param resourceConfigs    Config of all the resources that need to be rebalanced.
   *                           The tool throws Exception if any resource has no IS or broken/uninitialized IS.
   *                           The tool throws Exception if any resource is in full-auto mode.
   * @param existingAssignment The existing partition assignment of the input resources.
   * @param option             INIT or REASSIGN
   *                           INIT: Keep existing assignment. Only generate new partition assignment.
   *                           REASSIGN: Completely re-assign resources' partitions.
   * @param hardConstraints    Hard constraints for rebalancing.
   * @param softConstraints    Soft constraints for rebalancing.
   *                           Note that if the constraints are calculated based on the current partition assignment,
   *                           the assignment of the resources that are rebalanced should be excluded when setting constraints' state.
   * @return List of the IS that contains preference list or state map depends on the rebalance mode.
   **/
  public ResourcesStateMap calculateAssignment(List<ResourceConfig> resourceConfigs,
      ResourcesStateMap existingAssignment, RebalanceOption option,
      List<? extends AbstractRebalanceHardConstraint> hardConstraints,
      List<? extends AbstractRebalanceSoftConstraint> softConstraints) {
    // check the inputs
    for (ResourceConfig resourceConfig : resourceConfigs) {
      if (resourceConfig.getRebalanceConfig().getRebalanceMode()
          .equals(RebalanceConfig.RebalanceMode.FULL_AUTO)) {
        throw new HelixException(
            "Resources that in FULL_AUTO mode are not supported: " + resourceConfig
                .getResourceName());
      }
    }

    ConstraintRebalanceStrategy constraintBasedStrategy =
        new ConstraintRebalanceStrategy(hardConstraints, softConstraints);

    ResourcesStateMap resultAssignment = new ResourcesStateMap();

    for (ResourceConfig resourceConfig : resourceConfigs) {
      Map<String, Map<String, String>> preferredMapping = new HashMap<>();
      if (existingAssignment != null) {
        PartitionStateMap partitionStateMap = existingAssignment.getPartitionStateMap(resourceConfig.getResourceName());
        // keep existing assignment if rebalance option is INIT
        if (option.equals(RebalanceOption.INIT) && partitionStateMap != null) {
          for (Partition partition : partitionStateMap.getStateMap().keySet()) {
            preferredMapping.put(partition.getPartitionName(), partitionStateMap.getPartitionMap(partition));
          }
        }
      }

      StateModelDefinition stateModelDefinition =
          getStateModelDef(resourceConfig.getStateModelDefRef());
      constraintBasedStrategy.init(resourceConfig.getResourceName(),
          new ArrayList<>(resourceConfig.getPreferenceLists().keySet()), stateModelDefinition
              .getStateCountMap(_instanceConfigMap.size(),
                  Integer.parseInt(resourceConfig.getNumReplica())), Integer.MAX_VALUE);

      List<String> instanceNames = new ArrayList<>(_instanceConfigMap.keySet());
      ZNRecord znRecord = constraintBasedStrategy
          .computePartitionAssignment(instanceNames, instanceNames, preferredMapping, _dataCache);
      Map<String, Map<String, String>> stateMap = znRecord.getMapFields();
      // Construct resource states result
      PartitionStateMap newStateMap = new PartitionStateMap(resourceConfig.getResourceName());
      for (String partition : stateMap.keySet()) {
        newStateMap.setState(new Partition(partition), stateMap.get(partition));
      }
      resultAssignment.setState(resourceConfig.getResourceName(), newStateMap);
    }
    return resultAssignment;
  }

  private StateModelDefinition getStateModelDef(String stateModelDefRef) {
    if (_stateModelDefs.containsKey(stateModelDefRef)) {
      return _stateModelDefs.get(stateModelDefRef);
    }
    return BuiltInStateModelDefinitions.valueOf(stateModelDefRef).getStateModelDefinition();
  }

  /**
   * Since the tool is designed not to rely on ZK, if the application has customized state model,
   * it needs to register to the tool before calling for an assignment.
   *
   * @param stateModelDefRef
   * @param stateModelDefinition
   */
  public void registerCustomizedStateModelDef(String stateModelDefRef,
      StateModelDefinition stateModelDefinition) {
    _stateModelDefs.put(stateModelDefRef, stateModelDefinition);
  }
}

