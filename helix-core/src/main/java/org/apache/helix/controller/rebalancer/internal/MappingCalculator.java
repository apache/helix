package org.apache.helix.controller.rebalancer.internal;

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
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;

/**
 * Extends Rebalancer functionality by converting an IdealState to a ResourceAssignment.<br/>
 * <br/>
 * WARNING: this is an internal interface and is subject to change across releases
 */
public interface MappingCalculator<T extends BaseControllerDataProvider> {
  /**
   * Given an ideal state for a resource and the liveness of instances, compute the best possible
   * state assignment for each partition's replicas.
   * @param cache
   * @param idealState
   * @param resource
   * @param currentStateOutput
   *          Provides the current state and pending state transitions for all partitions
   * @return
   */
  ResourceAssignment computeBestPossiblePartitionState(
      T cache, IdealState idealState, Resource resource, CurrentStateOutput currentStateOutput);
}
