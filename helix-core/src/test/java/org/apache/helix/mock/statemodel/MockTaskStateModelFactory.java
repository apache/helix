package org.apache.helix.mock.statemodel;

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

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.helix.HelixManager;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.TaskFactory;


public class MockTaskStateModelFactory extends StateModelFactory<MockTaskStateModel> {
  private final HelixManager _manager;
  private final Map<String, TaskFactory> _taskFactoryRegistry;
  private final ScheduledExecutorService _taskExecutor;
  private final static int TASK_THREADPOOL_SIZE = 40;

  public MockTaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
    _manager = manager;
    _taskFactoryRegistry = taskFactoryRegistry;
    _taskExecutor = Executors.newScheduledThreadPool(TASK_THREADPOOL_SIZE, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        return new Thread(r, "TaskStateModel-thread-pool");
      }
    });
  }

  @Override
  public MockTaskStateModel createNewStateModel(String resourceName, String partitionKey) {
    return new MockTaskStateModel(_manager, _taskFactoryRegistry, _taskExecutor);
  }

  public void shutdown() {
    _taskExecutor.shutdown();
  }

  public boolean isShutdown() {
    return _taskExecutor.isShutdown();
  }

  public boolean isTerminated() {
    return _taskExecutor.isTerminated();
  }
}
