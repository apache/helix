package org.apache.helix.controller.rebalancer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.util.ConstraintBasedAssignment;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

public class CustomSemiAutoRebalancer implements Rebalancer, MappingCalculator {
  
  private static final Logger LOG = Logger.getLogger(AutoRebalancer.class);
  
  @Override
  public void init(HelixManager manager) {
  }

  @Override
  public IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    return currentIdealState;
  }
  
  @Override
  public ResourceAssignment computeBestPossiblePartitionState(
      ClusterDataCache cache, IdealState idealState, Resource resource,
      CurrentStateOutput currentStateOutput) {
    String stateModelDefName = idealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = cache.getStateModelDef(stateModelDefName);
    ResourceAssignment partitionMapping = new ResourceAssignment(resource.getResourceName());
    if (LOG.isDebugEnabled()) {
      LOG.debug("before, resource: " + resource.getResourceName() + ", idealState " + idealState.toString());
    }
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> currentStateMap = currentStateOutput.getCurrentStateMap(resource.getResourceName(), partition);
      Set<String> disabledInstancesForPartition = cache.getDisabledInstancesForPartition(partition.toString());
      List<String> preferenceList = ConstraintBasedAssignment.getPreferenceList(cache, partition, idealState, stateModelDef);
      // first check error/drop case
      Map<String, String> errorStateMap = errorOrDropForPartition(
          stateModelDef, preferenceList, currentStateMap,
          disabledInstancesForPartition, idealState.isEnabled());
      // then check whether idealState has preferred state
      Map<String, String> idealStateMap = idealState.getInstanceStateMap(partition.getPartitionName());
      Map<String, String> preferredStateForPartition = computeCustomizedBestStateForPartition(
          cache, errorStateMap, idealStateMap, currentStateMap,
          disabledInstancesForPartition, idealState.isEnabled());
      if (LOG.isDebugEnabled()) {
        LOG.debug("preferredStateForPartition " + preferredStateForPartition.toString());
      }
      // lastly according to state preference list to calc best state for partition
      Map<String, String> bestStateForPartition = ConstraintBasedAssignment.computeCustomAutoBestStateForPartition(
                  cache, stateModelDef,
              preferenceList, currentStateMap,
              preferredStateForPartition,
              disabledInstancesForPartition,
              idealState.isEnabled());
      partitionMapping.addReplicaMap(partition, bestStateForPartition);
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug(String.format("%s, bestPossiblePartitionState: ", resource.getResourceName(), partitionMapping.toString()));
    }
    
    return partitionMapping;
  }
  
  private Map<String, String> errorOrDropForPartition(
      StateModelDefinition stateModelDef,
      List<String> instancePreferenceList,
      Map<String, String> currentStateMap,
      Set<String> disabledInstancesForPartition, 
      boolean isResourceEnabled
      ) {
    Map<String, String> instanceStateMap = new HashMap<String, String>();
    // if the ideal state is deleted, instancePreferenceList will be empty
    // and we should drop all resources.
    if (currentStateMap != null) {
      for (String instance : currentStateMap.keySet()) {
        if ((instancePreferenceList == null || !instancePreferenceList.contains(instance))
            && !disabledInstancesForPartition.contains(instance) && isResourceEnabled) {
          instanceStateMap.put(instance, HelixDefinedState.DROPPED.toString());
        } else if ((currentStateMap.get(instance) == null || !currentStateMap.get(instance).equals(HelixDefinedState.ERROR.name()))
            && (disabledInstancesForPartition.contains(instance) || !isResourceEnabled)) {
          // if disabled and not in ERROR state, transit to initial-state (e.g. OFFLINE)
          instanceStateMap.put(instance, stateModelDef.getInitialState());
        }
      }
    }
    
    return instanceStateMap;
  }

  /**
   * compute best state for resource in CUSTOMIZED ideal state mode
   */
  private Map<String, String> computeCustomizedBestStateForPartition(
      ClusterDataCache cache,
      Map<String, String> errorStateMap,
      Map<String, String> idealStateMap,
      Map<String, String> currentStateMap,
      Set<String> disabledInstancesForPartition, boolean isResourceEnabled) {
    Map<String, String> instanceStateMap = errorStateMap;

    // ideal state does not contain this partition
    if (idealStateMap == null) {
      return instanceStateMap;
    }

    Map<String, LiveInstance> liveInstancesMap = cache.getLiveInstances();
    for (String instance : idealStateMap.keySet()) {
      boolean notInErrorState = currentStateMap == null || currentStateMap.get(instance) == null
          || !currentStateMap.get(instance).equals(HelixDefinedState.ERROR.toString());

      boolean enabled = !disabledInstancesForPartition.contains(instance) && isResourceEnabled;

      if (liveInstancesMap.containsKey(instance) && notInErrorState && enabled && idealStateMap.containsKey(instance)) {
        instanceStateMap.put(instance, idealStateMap.get(instance));
      }
    }
    
    return instanceStateMap;
  }
}
