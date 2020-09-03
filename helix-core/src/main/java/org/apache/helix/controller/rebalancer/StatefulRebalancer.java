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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;


/**
 * Allows one to come up with custom implementation of a stateful rebalancer.<br/>
 */
public interface StatefulRebalancer<T extends BaseControllerDataProvider> {

  /**
   * Reset the rebalancer to the initial state.
   */
  void reset();

  /**
   * Release all the resources and clean up all the rebalancer state.
   */
  void close();

  /**
   * Compute the new IdealStates for all the input resources. The IdealStates include both new
   * partition assignment (in the listFiles) and the new replica state mapping (in the mapFields).
   * @param clusterData The Cluster status data provider.
   * @param resourceMap A map containing all the rebalancing resources.
   * @param currentStateOutput The present Current States of the resources.
   * @return A map of the new IdealStates with the resource name as key.
   */
  Map<String, IdealState> computeNewIdealStates(T clusterData, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput) throws HelixRebalanceException;
}
