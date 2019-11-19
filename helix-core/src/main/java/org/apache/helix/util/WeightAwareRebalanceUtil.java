package org.apache.helix.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.RebalanceConfig;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceHardConstraint;
import org.apache.helix.api.rebalancer.constraint.AbstractRebalanceSoftConstraint;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.strategy.ConstraintRebalanceStrategy;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.StateModelDefinition;

/**
 * A rebalance tool that generate an resource partition assignment based on the input.
 * Note the assignment won't be automatically applied to the cluster. Users are supposed to
 * apply the change.
 *
 * @see org.apache.helix.examples.WeightAwareRebalanceUtilExample WeightAwareRebalanceUtilExample
 */
public class WeightAwareRebalanceUtil {
  private final ClusterConfig _clusterConfig;
  private final Map<String, InstanceConfig> _instanceConfigMap = new HashMap<>();
  // For the possible customized state models.
  private final Map<String, StateModelDefinition> _stateModelDefs = new HashMap<>();
  private final ResourceControllerDataProvider _dataCache;

  private enum RebalanceOption {
    INCREMENTAL,
    FULL
  }

  /**
   * Init the rebalance util with cluster and instances information.
   *
   * Note that it is not required to put any configuration items in these configs.
   * However, in order to do topology aware rebalance, users need to set topology information such as Domain, fault zone, and TopologyAwareEnabled.
   *
   * The other config items will not be read or processed by the util.
   *
   * @param clusterConfig
   * @param instanceConfigs InstanceConfigs for all assignment candidates.
   *                        Note that all instances will be treated as enabled and alive during the calculation.
   */
  public WeightAwareRebalanceUtil(ClusterConfig clusterConfig,
      List<InstanceConfig> instanceConfigs) {
    for (InstanceConfig instanceConfig : instanceConfigs) {
      // ensure the instance is enabled
      instanceConfig.setInstanceEnabled(true);
      _instanceConfigMap.put(instanceConfig.getInstanceName(), instanceConfig);
    }
    // ensure no instance is disabled
    clusterConfig.setDisabledInstances(Collections.<String, String>emptyMap());
    _clusterConfig = clusterConfig;

    _dataCache = new ResourceControllerDataProvider();
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
   * Generate partition assignments for all new resources or partitions that have not been assigned yet.
   * Note that a partition assignment that does not fit the state model will still be recalculated.
   * For example, if the replica requirement is 3, but one partition has only 2 replicas, this partition will still
   * be rebalanced even existing assignment exists.
   *
   * @param resourceConfigs    Config of all the resources that need to be rebalanced.
   *                           The tool throws Exception if any resource has no IS or broken/uninitialized IS.
   *                           The tool throws Exception if any resource is in full-auto mode.
   *                           Following fields are required by the tool:
   *                           1. ResourceName
   *                           2. StateModelDefRef
   *                           3. PreferenceLists, which includes all partitions in the resource
   *                           4. NumReplica
   * @param existingAssignment The existing partition assignment of the resources specified in param resourceConfigs.
   *                           Unrelated resource assignment will be discarded.
   * @param hardConstraints    Hard constraints for rebalancing.
   * @param softConstraints    Soft constraints for rebalancing.
   *
   * @return List of the IS that contains preference list and suggested state map
   **/
  public ResourcesStateMap buildIncrementalRebalanceAssignment(List<ResourceConfig> resourceConfigs,
      ResourcesStateMap existingAssignment,
      List<? extends AbstractRebalanceHardConstraint> hardConstraints,
      List<? extends AbstractRebalanceSoftConstraint> softConstraints) {
    return calculateAssignment(resourceConfigs, existingAssignment, RebalanceOption.INCREMENTAL,
        hardConstraints, softConstraints);
  }

  /**
   * Re-calculate the partition assignments for all the resources specified in resourceConfigs list.
   *
   * @param resourceConfigs    Config of all the resources that need to be rebalanced.
   *                           The tool throws Exception if any resource has no IS or broken/uninitialized IS.
   *                           The tool throws Exception if any resource is in full-auto mode.
   *                           Following fields are required by the tool:
   *                           1. ResourceName
   *                           2. StateModelDefRef
   *                           3. PreferenceLists, which includes all partitions in the resource
   *                           4. NumReplica
   * @param preferredAssignment A set of preferred partition assignments for the resources specified in param resourceConfigs.
   *                            The preference is not guaranteed.
   * @param hardConstraints    Hard constraints for rebalancing.
   * @param softConstraints    Soft constraints for rebalancing.
   *
   * @return List of the IS that contains preference list and suggested state map
   **/
  public ResourcesStateMap buildFullRebalanceAssignment(List<ResourceConfig> resourceConfigs,
      ResourcesStateMap preferredAssignment,
      List<? extends AbstractRebalanceHardConstraint> hardConstraints,
      List<? extends AbstractRebalanceSoftConstraint> softConstraints) {
    return calculateAssignment(resourceConfigs, preferredAssignment, RebalanceOption.FULL,
        hardConstraints, softConstraints);
  }

  /**
   * The method to generate partition assignment mappings.
   *
   * @param resourceConfigs    Config of all the resources that need to be rebalanced.
   *                           The tool throws Exception if any resource has no IS or broken/uninitialized IS.
   *                           The tool throws Exception if any resource is in full-auto mode.
   *                           Following fields are required by the tool:
   *                           1. ResourceName
   *                           2. StateModelDefRef
   *                           3. PreferenceLists, which includes all partitions in the resource
   *                           4. NumReplica
   * @param existingAssignment The existing partition assignment of the resources specified in param resourceConfigs.
   * @param option             INCREMENTAL or FULL
   *                           INCREMENTAL: Keep existing assignment. Only generate new partition assignment.
   *                           FULL: Completely re-assign resources' partitions.
   * @param hardConstraints    Hard constraints for rebalancing.
   * @param softConstraints    Soft constraints for rebalancing.
   *
   * @return List of the IS that contains preference list and suggested state map
   **/
  private ResourcesStateMap calculateAssignment(List<ResourceConfig> resourceConfigs,
      ResourcesStateMap existingAssignment, RebalanceOption option,
      List<? extends AbstractRebalanceHardConstraint> hardConstraints,
      List<? extends AbstractRebalanceSoftConstraint> softConstraints) {
    // check the inputs
    for (ResourceConfig resourceConfig : resourceConfigs) {
      RebalanceConfig.RebalanceMode rebalanceMode =
          resourceConfig.getRebalanceConfig().getRebalanceMode();
      if (rebalanceMode.equals(RebalanceConfig.RebalanceMode.FULL_AUTO)) {
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
        // keep existing assignment if rebalance option is INCREMENTAL
        if (option.equals(RebalanceOption.INCREMENTAL) && partitionStateMap != null) {
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
