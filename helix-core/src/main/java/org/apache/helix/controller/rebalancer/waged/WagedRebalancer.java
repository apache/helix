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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintsRebalanceAlgorithm;
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
 * @see <a href="https://github.com/apache/helix/wiki/Design-Proposal---Weight-Aware-Globally-Even-Distribute-Rebalancer">
 * Design Document
 * </a>
 */
public class WagedRebalancer {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  // --------- The following fields are placeholders and need replacement. -----------//
  // TODO Shall we make the metadata store a static threadlocal object as well to avoid reinitialization?
  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;
  // ------------------------------------------------------------------------------------//

  // The cluster change detector is a stateful object. Make it static to avoid unnecessary
  // reinitialization.
  private static final ThreadLocal<ResourceChangeDetector> CHANGE_DETECTOR_THREAD_LOCAL =
      new ThreadLocal<>();
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;

  private ResourceChangeDetector getChangeDetector() {
    if (CHANGE_DETECTOR_THREAD_LOCAL.get() == null) {
      CHANGE_DETECTOR_THREAD_LOCAL.set(new ResourceChangeDetector());
    }
    return CHANGE_DETECTOR_THREAD_LOCAL.get();
  }

  public WagedRebalancer(HelixManager helixManager) {
    // TODO init the metadata store according to their requirement when integrate, or change to final static method if possible.
    _assignmentMetadataStore = new AssignmentMetadataStore();
    // TODO init the algorithm according to the requirement when integrate.
    _rebalanceAlgorithm = new ConstraintsRebalanceAlgorithm();

    // Use the mapping calculator in DelayedAutoRebalancer for calculating the final assignment
    // output.
    // This calculator will translate the best possible assignment into an applicable state mapping
    // based on the current states.
    // TODO abstract and separate the mapping calculator logic from the DelayedAutoRebalancer
    _mappingCalculator = new DelayedAutoRebalancer();
  }

  /**
   * Compute the new IdealStates for all the resources input. The IdealStates include both the new
   * partition assignment (in the listFiles) and the new replica state mapping (in the mapFields).
   * @param clusterData        The Cluster status data provider.
   * @param resourceMap        A map containing all the rebalancing resources.
   * @param currentStateOutput The present Current State of the cluster.
   * @return A map containing the computed new IdealStates.
   */
  public Map<String, IdealState> computeNewIdealStates(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput)
      throws HelixRebalanceException {
    return new HashMap<>();
  }
}
