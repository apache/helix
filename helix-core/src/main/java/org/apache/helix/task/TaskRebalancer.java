package org.apache.helix.task;

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
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.controller.rebalancer.internal.MappingCalculator;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Resource;
import org.apache.helix.model.ResourceAssignment;

/**
 * Abstract rebalancer class for the {@code Task} state model.
 */
public abstract class TaskRebalancer extends AbstractTaskDispatcher
    implements Rebalancer<WorkflowControllerDataProvider>,
    MappingCalculator<WorkflowControllerDataProvider> {

  @Override
  public void init(HelixManager manager) {
    _manager = manager;
  }

  @Override
  public abstract ResourceAssignment computeBestPossiblePartitionState(WorkflowControllerDataProvider clusterData,
      IdealState taskIs, Resource resource, CurrentStateOutput currStateOutput);



  @Override
  public IdealState computeNewIdealState(String resourceName, IdealState currentIdealState,
      CurrentStateOutput currentStateOutput, WorkflowControllerDataProvider clusterData) {
    // All of the heavy lifting is in the ResourceAssignment computation,
    // so this part can just be a no-op.
    return currentIdealState;
  }

}
