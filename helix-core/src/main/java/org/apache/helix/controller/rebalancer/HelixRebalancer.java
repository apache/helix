package org.apache.helix.controller.rebalancer;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.stages.ResourceCurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.ResourceAssignment;

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

/**
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the resource assignment for a resource in this method.<br/>
 */
public interface HelixRebalancer {
  /**
   * Initialize the rebalancer with a HelixManager and ControllerContextProvider if necessary
   * @param manager HelixManager instance
   * @param contextProvider An object that supports getting and setting context across pipeline runs
   */
  public void init(HelixManager helixManager, ControllerContextProvider contextProvider);

  /**
   * Given an ideal state for a resource and liveness of participants, compute a assignment of
   * instances and states to each partition of a resource. This method provides all the relevant
   * information needed to rebalance a resource. If you need additional information use
   * manager.getAccessor to read and write the cluster data. This allows one to compute the
   * ResourceAssignment according to app-specific requirements.<br/>
   * <br/>
   * Say that you have:<br/>
   * 
   * <pre>
   * class MyRebalancerConfig implements RebalancerConfig
   * </pre>
   * 
   * as your rebalancer config. To get a typed version, you can do the following:<br/>
   * 
   * <pre>
   * MyRebalancerConfig config = BasicRebalancerConfig.convert(rebalancerConfig,
   *     MyRebalancerConfig.class);
   * </pre>
   * @param idealState the ideal state that defines how a resource should be rebalanced
   * @param rebalancerConfig the properties of the resource for which a mapping will be computed
   * @param prevAssignment the previous ResourceAssignment of this cluster, or null if none
   * @param cluster complete snapshot of the cluster
   * @param currentState the current states of all partitions
   */
  public ResourceAssignment computeResourceMapping(IdealState idealState,
      RebalancerConfig rebalancerConfig, ResourceAssignment prevAssignment, Cluster cluster,
      ResourceCurrentState currentState);
}
