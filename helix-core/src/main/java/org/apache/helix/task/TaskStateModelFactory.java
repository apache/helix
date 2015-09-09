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

import java.util.Deque;
import java.util.Map;

import org.apache.helix.HelixManager;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Factory class for {@link TaskStateModel}.
 */
public class TaskStateModelFactory extends StateTransitionHandlerFactory<TaskStateModel> {

  private final HelixManager _manager;
  private final Map<String, TaskFactory> _taskFactoryRegistry;
  private final Deque<TaskStateModel> _taskStateModels;

  public TaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    _manager = manager;
    _taskFactoryRegistry = ImmutableMap.copyOf(taskFactoryRegistry);
    _taskStateModels = Lists.newLinkedList();
  }

  @Override
  public TaskStateModel createStateTransitionHandler(ResourceId resourceId, PartitionId partitionId) {
    final TaskStateModel taskStateModel = new TaskStateModel(_manager, _taskFactoryRegistry);
    _taskStateModels.push(taskStateModel);
    return taskStateModel;
  }

  /**
   * Shutdown this {@link TaskStateModelFactory} instance.
   *
   * <p>
   *   This method shuts down all of the {@link TaskStateModel} instances created by this
   *   {@link TaskStateModelFactory} instance.
   * </p>
   */
  public void shutdown() {
    while (!_taskStateModels.isEmpty()) {
      TaskStateModel taskStateModel = _taskStateModels.pop();
      taskStateModel.shutdown();
    }
  }
}
