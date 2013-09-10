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

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.controller.stages.NewCurrentStateOutput;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.model.StateModelDefinition;

/**
 * Allows one to come up with custom implementation of a rebalancer.<br/>
 * This will be invoked on all changes that happen in the cluster.<br/>
 * Simply return the newIdealState for a resource in this method.<br/>
 */
public interface NewRebalancer {

  /**
   * Given a resource, existing mapping, and liveness of resources, compute a new mapping of
   * resources.
   * @param resource the resource for which a mapping will be computed
   * @param cluster a snapshot of the entire cluster state
   * @param stateModelDef the state model for which to rebalance the resource
   * @param currentStateOutput a combination of the current states and pending current states
   */
  ResourceAssignment computeResourceMapping(final ResourceConfig resourceConfig,
      final Cluster cluster, final StateModelDefinition stateModelDef,
      final NewCurrentStateOutput currentStateOutput);
}
