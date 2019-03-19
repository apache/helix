package org.apache.helix.rest.server.service;

import org.apache.helix.rest.server.json.cluster.ClusterInfo;
import org.apache.helix.rest.server.json.cluster.ClusterTopology;


/**
 * A rest wrapper service that provides information about cluster
 * TODO add more business logic and simplify the workload on ClusterAccessor
 */
public interface ClusterService {
  /**
   * Get cluster topology
   * @param cluster
   * @return
   */
  ClusterTopology getClusterTopology(String cluster);

  /**
   * Get cluster basic information
   * @param clusterId
   * @return
   */
  ClusterInfo getClusterInfo(String clusterId);
}
