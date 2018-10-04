package org.apache.helix.controller.rebalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MaintenanceRebalancer extends SemiAutoRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(MaintenanceRebalancer.class);

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    LOG.info(String
        .format("Start computing ideal state for resource %s in maintenance mode.", resourceName));
    Map<Partition, Map<String, String>> currentStateMap =
        currentStateOutput.getCurrentStateMap(resourceName);
    if (currentStateMap == null || currentStateMap.size() == 0) {
      LOG.warn(String
          .format("No new partition will be assigned for %s in maintenance mode", resourceName));

      // Clear all preference lists, if the resource has not yet been rebalanced,
      // leave it as is
      for (List<String> pList : currentIdealState.getPreferenceLists().values()) {
        pList.clear();
      }
      return currentIdealState;
    }

    // One principal is to prohibit DROP -> OFFLINE and OFFLINE -> DROP state transitions.
    // Derived preference list from current state with state priority
    for (Partition partition : currentStateMap.keySet()) {
      Map<String, String> stateMap = currentStateMap.get(partition);
      List<String> preferenceList = new ArrayList<>(stateMap.keySet());
      Collections.sort(preferenceList, new PreferenceListNodeComparator(stateMap,
          clusterData.getStateModelDef(currentIdealState.getStateModelDefRef()),
          Collections.<String>emptyList()));
      currentIdealState.setPreferenceList(partition.getPartitionName(), preferenceList);
    }
    LOG.info(String
        .format("End computing ideal state for resource %s in maintenance mode.", resourceName));
    return currentIdealState;
  }
}
