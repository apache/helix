package org.apache.helix.tools.ClusterVerifiers;

import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.AssignmentMetadataStore;
import org.apache.helix.controller.rebalancer.waged.WagedRebalancer;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;


/**
 * A Dryrun WAGED rebalancer that only calculates the assignment based on the cluster status but
 * never update the rebalancer assignment metadata.
 * This rebalacer is used in the verifiers or tests.
 */
public class DryrunWagedRebalancer extends WagedRebalancer {
  public DryrunWagedRebalancer(String metadataStoreAddrs, String clusterName,
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    super(new ReadOnlyAssignmentMetadataStore(metadataStoreAddrs, clusterName),
        ConstraintBasedAlgorithmFactory.getInstance(preferences));
  }

  @Override
  protected Map<String, ResourceAssignment> computeBestPossibleAssignment(
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap,
      Set<String> activeNodes, CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    return getBestPossibleAssignment(getAssignmentMetadataStore(), currentStateOutput,
        resourceMap.keySet());
  }
}

class ReadOnlyAssignmentMetadataStore extends AssignmentMetadataStore {
  ReadOnlyAssignmentMetadataStore(String metadataStoreAddrs, String clusterName) {
    super(new ZkBucketDataAccessor(metadataStoreAddrs), clusterName, false);
  }

  @Override
  public void persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    // Do nothing. It is a readonly store.
  }

  @Override
  public void persistBestPossibleAssignment(
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    // Do nothing. It is a readonly store.
  }
}
