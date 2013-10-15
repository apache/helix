package org.apache.helix.controller.rebalancer;

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

import org.apache.helix.HelixManager;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;

/**
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the newIdealState for a resource in this method.<br/>
 * <br/>
 * Deprecated. Use {@link HelixRebalancer} instead.
 */
@Deprecated
public interface Rebalancer {
  /**
   * Initialize the rebalancer with a HelixManager if necessary
   * @param manager
   */
  public void init(HelixManager manager);

  /**
   * Given an ideal state for a resource and liveness of instances, compute a assignment of
   * instances and states to each partition of a resource. This method provides all the relevant
   * information needed to rebalance a resource. If you need additional information use
   * manager.getAccessor to read the cluster data. This allows one to compute the newIdealState
   * according to app specific requirements.
   * @param resourceName the resource for which a mapping will be computed
   * @param currentIdealState the IdealState that corresponds to this resource
   * @param currentStateOutput the current states of all partitions
   * @param clusterData cache of the cluster state
   */
  public IdealState computeResourceMapping(final String resourceName,
      final IdealState currentIdealState, final CurrentStateOutput currentStateOutput,
      final ClusterDataCache clusterData);
}
