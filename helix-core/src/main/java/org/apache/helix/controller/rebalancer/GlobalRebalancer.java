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

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;

import java.util.Map;

/**
 * Interface of the global rebalancer which is designed to compute and optimize multiple
 * resources' IdealStates simultaneously.
 */
public interface GlobalRebalancer<T extends BaseControllerDataProvider> {
  enum RebalanceFailureType {
    INVALID_CLUSTER_STATUS,
    INVALID_REBALANCER_STATUS,
    FAILED_TO_CALCULATE,
    UNKNOWN_FAILURE
  }

  class RebalanceFailureReason {
    private final static String DEFAULT_REASON_MESSAGE = "No detail";
    private final RebalanceFailureType _type;
    private final String _reason;

    public RebalanceFailureReason(RebalanceFailureType type) {
      this(type, DEFAULT_REASON_MESSAGE);
    }

    public RebalanceFailureReason(RebalanceFailureType type, String reason) {
      _type = type;
      _reason = reason;
    }

    public RebalanceFailureType getType() {
      return _type;
    }

    public String getReason() {
      return _reason;
    }
  }

  /**
   * Compute the new IdealStates for all the resources input. The IdealStates include both the new
   * partition assignment (in the listFiles) and the new replica state mapping (in the mapFields).
   *
   * @param clusterData        The Cluster status data provider.
   * @param resourceMap        A map containing all the rebalancing resources.
   * @param currentStateOutput The present Current State of the cluster.
   * @return A map containing the computed new IdealStates.
   */
  Map<String, IdealState> computeNewIdealStates(T clusterData, Map<String, Resource> resourceMap,
      final CurrentStateOutput currentStateOutput);

  /**
   * @return Details of the rebalance failure. Null if the computing is done successfully.
   */
  RebalanceFailureReason getFailureReason();
}
