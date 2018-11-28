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
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;

/**
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the newIdealState for a resource in this method.<br/>
 */
public interface Rebalancer<T extends BaseControllerDataProvider> {
  void init(HelixManager manager);

  /**
   * This method provides all the relevant information needed to rebalance a resource.
   * If you need additional information use manager.getAccessor to read the cluster data.
   * This allows one to compute the newIdealState according to app specific requirement.
   * @param resourceName Name of the resource to be rebalanced
   * @param currentIdealState
   * @param currentStateOutput
   *          Provides the current state and pending state transition for all
   *          partitions
   * @param clusterData Provides additional methods to retrieve cluster data.
   * @return
   */
   IdealState computeNewIdealState(String resourceName,
      IdealState currentIdealState, final CurrentStateOutput currentStateOutput,
      T clusterData);
}
