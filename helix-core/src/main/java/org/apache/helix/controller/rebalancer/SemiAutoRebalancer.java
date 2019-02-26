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

import org.apache.helix.controller.BaseControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;

/**
 * This is a Rebalancer specific to semi-automatic mode. It is tasked with computing the ideal
 * state of a resource based on a predefined preference list of instances willing to accept
 * replicas.
 * The input is the optional current assignment of partitions to instances, as well as the required
 * existing instance preferences.
 * The output is a mapping based on that preference list, i.e. partition p has a replica on node k
 * with state s.
 *
 * NOTE: because SemiAutoRebalancer is used in both Resource controller and Workflow controller,
 * We need to use template as it's data provider type
 */
public class SemiAutoRebalancer<T extends BaseControllerDataProvider>
    extends AbstractRebalancer<T> {

  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, T clusterData) {
    return currentIdealState;
  }
}
