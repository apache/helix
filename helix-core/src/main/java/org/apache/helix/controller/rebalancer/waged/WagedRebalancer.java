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
import org.apache.helix.controller.changedetector.ChangeDetector;
import org.apache.helix.controller.changedetector.ResourceChangeDetector;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.DelayedAutoRebalancer;
import org.apache.helix.controller.rebalancer.GlobalRebalancer;
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
public class WagedRebalancer implements GlobalRebalancer<ResourceControllerDataProvider> {
  private static final Logger LOG = LoggerFactory.getLogger(WagedRebalancer.class);

  // The cluster change detector is a stateful object,
  // make it static to avoid unnecessary cleanup and reconstruct.
  private static final ThreadLocal<ChangeDetector> _changeDetector = new ThreadLocal<>();

  private final AssignmentMetadataStore _assignmentMetadataStore;
  private final RebalanceAlgorithm _rebalanceAlgorithm;
  private final MappingCalculator<ResourceControllerDataProvider> _mappingCalculator;

  private ChangeDetector getChangeDetector() {
    if (_changeDetector.get() == null) {
      _changeDetector.set(new ResourceChangeDetector());
    }
    return _changeDetector.get();
  }

  public WagedRebalancer(HelixManager helixManager) {
    // Use the mapping calculator in DelayedAutoRebalancer for calculating the final assignment output.
    // TODO abstract and separate the mapping calculator logic from the DelayedAutoRebalancer
    _mappingCalculator = new DelayedAutoRebalancer();
    // TODO init the metadata store according to their requirement when integrate, or change to final static method if possible.
    _assignmentMetadataStore = new AssignmentMetadataStore();
    // TODO init the algorithm according to the requirement when integrate.
    _rebalanceAlgorithm = new ConstraintsRebalanceAlgorithm();
  }

  @Override
  public Map<String, IdealState> computeNewIdealStates(ResourceControllerDataProvider clusterData,
      Map<String, Resource> resourceMap, final CurrentStateOutput currentStateOutput) {
    return new HashMap<>();
  }

  @Override
  public RebalanceFailureReason getFailureReason() {
    // TODO record and return the right failure information
    return new RebalanceFailureReason(RebalanceFailureType.UNKNOWN_FAILURE);
  }
}
