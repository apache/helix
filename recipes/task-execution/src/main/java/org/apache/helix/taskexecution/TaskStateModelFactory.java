package org.apache.helix.taskexecution;

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

import org.apache.helix.participant.statemachine.StateModelFactory;

public class TaskStateModelFactory extends StateModelFactory<TaskStateModel> {
  private final String _workerId;
  private final TaskFactory _taskFactory;
  private TaskResultStore _taskResultStore;

  public TaskStateModelFactory(String workerId, TaskFactory taskFactory,
      TaskResultStore taskResultStore) {
    _workerId = workerId;
    _taskFactory = taskFactory;
    _taskResultStore = taskResultStore;
  }

  @Override
  public TaskStateModel createNewStateModel(String resource, String partition) {
    TaskStateModel model = new TaskStateModel(_workerId, partition, _taskFactory, _taskResultStore);
    return model;
  }
}
