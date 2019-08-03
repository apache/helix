package org.apache.helix.controller.rebalancer.waged;

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

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.model.ClusterModel;
import org.apache.helix.model.IdealState;

import java.util.Map;
import java.util.Set;

/**
 * A placeholder before we have the implementation.
 *
 * The data provider generates the Cluster Model based on the controller's data cache.
 */
public class ClusterDataProvider {

  /**
   * @param dataProvider           The controller's data cache.
   * @param activeInstances        The logical active instances that will be used in the calculation. Note
   *                               This list can be different from the real active node list according to
   *                               the rebalancer logic.
   * @param clusterChanges         All the cluster changes that happened after the previous rebalance.
   * @param baselineAssignment     The persisted Baseline assignment.
   * @param bestPossibleAssignment The persisted Best Possible assignment that was generated in the
   *                               previous rebalance.
   * @return The cluster model as the input for the upcoming rebalance.
   */
  protected static ClusterModel generateClusterModel(ResourceControllerDataProvider dataProvider,
      Set<String> activeInstances, Map<ClusterDataDetector.ChangeType, Set<String>> clusterChanges,
      Map<String, IdealState> baselineAssignment, Map<String, IdealState> bestPossibleAssignment) {
    // TODO finish the implementation.
    return null;
  }
}
