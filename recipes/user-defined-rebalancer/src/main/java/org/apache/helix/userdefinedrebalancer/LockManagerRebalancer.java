package org.apache.helix.userdefinedrebalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;
import org.apache.log4j.Logger;

public class LockManagerRebalancer implements Rebalancer {
  private static final Logger LOG = Logger.getLogger(LockManagerRebalancer.class);

  @Override
  public void init(HelixManager manager) {
    // do nothing; this rebalancer is independent of the manager
  }

  /**
   * This rebalancer is invoked whenever there is a change in the cluster, including when new
   * participants join or leave, or the configuration of any participant changes. It is written
   * specifically to handle assignment of locks to nodes under the very simple lock-unlock state
   * model.
   */
  @Override
  public ResourceAssignment computeResourceMapping(Resource resource, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, ClusterDataCache clusterData) {
    // Initialize an empty mapping of locks to participants
    ResourceAssignment assignment = new ResourceAssignment(resource.getResourceName());

    // Get the list of live participants in the cluster
    List<String> liveParticipants = new ArrayList<String>(clusterData.getLiveInstances().keySet());

    // Get the state model (should be a simple lock/unlock model) and the highest-priority state
    String stateModelName = currentIdealState.getStateModelDefRef();
    StateModelDefinition stateModelDef = clusterData.getStateModelDef(stateModelName);
    if (stateModelDef.getStatesPriorityList().size() < 1) {
      LOG.error("Invalid state model definition. There should be at least one state.");
      return assignment;
    }
    String lockState = stateModelDef.getStatesPriorityList().get(0);

    // Count the number of participants allowed to lock each lock
    String stateCount = stateModelDef.getNumInstancesPerState(lockState);
    int lockHolders = 0;
    try {
      // a numeric value is a custom-specified number of participants allowed to lock the lock
      lockHolders = Integer.parseInt(stateCount);
    } catch (NumberFormatException e) {
      LOG.error("Invalid state model definition. The lock state does not have a valid count");
      return assignment;
    }

    // Fairly assign the lock state to the participants using a simple mod-based sequential
    // assignment. For instance, if each lock can be held by 3 participants, lock 0 would be held
    // by participants (0, 1, 2), lock 1 would be held by (1, 2, 3), and so on, wrapping around the
    // number of participants as necessary.
    // This assumes a simple lock-unlock model where the only state of interest is which nodes have
    // acquired each lock.
    int i = 0;
    for (Partition partition : resource.getPartitions()) {
      Map<String, String> replicaMap = new HashMap<String, String>();
      for (int j = i; j < i + lockHolders; j++) {
        int participantIndex = j % liveParticipants.size();
        String participant = liveParticipants.get(participantIndex);
        // enforce that a participant can only have one instance of a given lock
        if (!replicaMap.containsKey(participant)) {
          replicaMap.put(participant, lockState);
        }
      }
      assignment.addReplicaMap(partition, replicaMap);
      i++;
    }
    return assignment;
  }
}
