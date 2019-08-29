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

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.GlobalRebalancer;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A placeholder before we have the implementation.
 * Weight-Aware Globally-Even Distribute Rebalancer.
 *
 * @see <a href="Design Document">https://github.com/apache/helix/wiki/Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer</a>
 */
public class WagedRebalancer implements GlobalRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  @Override
  public void init(HelixManager manager) { }

  @Override
  public Map<String, IdealState> computeNewIdealState(CurrentStateOutput currentStateOutput,
      ResourceControllerDataProvider clusterData, Map<String, Resource> resourceMap)
      throws HelixException {
    return new HashMap<>();
  }

  @Override
  public RebalanceFailureReason getFailureReason() {
    return new RebalanceFailureReason(RebalanceFailureType.UNKNOWN_FAILURE);
  }
}
