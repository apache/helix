package org.apache.helix.task.assigner;

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

import java.util.Collection;
import java.util.Map;

import org.apache.helix.task.AssignableInstanceManager;
import org.apache.helix.task.TaskConfig;

public interface TaskAssigner {

  /**
   * Assign a collection of tasks on a collection of assignableInstances.
   * When an assignment decision is made, AssignableInstance.assign() must be called for the
   * instance to modify its internal capacity profile. Note that all tasks will be treated as
   * belonging to the DEFAULT type.
   * @param assignableInstances AssignableInstances
   * @param tasks TaskConfigs of the same quota type
   * @return taskID -> TaskAssignmentResult mappings
   */
  Map<String, TaskAssignResult> assignTasks(Iterable<AssignableInstance> assignableInstances,
      Iterable<TaskConfig> tasks);

  /**
   * Assign a collection of tasks on a collection of assignableInstances.
   * When an assignment decision is made, AssignableInstance.assign() must be called for the
   * instance to modify its internal capacity profile.
   * @param assignableInstances AssignableInstances
   * @param tasks TaskConfigs of the same quota type
   * @param quotaType quota type of the tasks
   * @return taskID -> TaskAssignmentResult mappings
   */
  Map<String, TaskAssignResult> assignTasks(Iterable<AssignableInstance> assignableInstances,
      Iterable<TaskConfig> tasks, String quotaType);

  /**
   * Assign a collection of tasks on AssignableInstanceManager
   * When an assignment decision is made, AssignableInstance.assign() must be called for the
   * instance to modify its internal capacity profile.
   * @param assignableInstanceManager AssignableInstanceManager
   * @param instances instances to assign to (need this to honor instance group tags)
   * @param tasks TaskConfigs of the same quota type
   * @param quotaType quota type of the tasks
   * @return taskID -> TaskAssignmentResult mappings
   */
  Map<String, TaskAssignResult> assignTasks(AssignableInstanceManager assignableInstanceManager,
      Collection<String> instances, Iterable<TaskConfig> tasks, String quotaType);
}
