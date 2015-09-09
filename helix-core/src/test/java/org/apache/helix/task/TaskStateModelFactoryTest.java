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

import java.util.List;
import java.util.Map;

import org.apache.helix.Mocks;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Unit tests for {@link TaskStateModelFactory}.
 *
 * @author liyinan926
 */
@Test(groups = { "org.apache.helix.task" })
public class TaskStateModelFactoryTest {

  @Test
  public void testCreateAndShutdownTaskStateModels() {
    Map<String, TaskFactory> taskFactoryMap = Maps.newHashMap();
    taskFactoryMap.put("Test", new TestTaskFactory());
    TaskStateModelFactory taskStateModelFactory = new TaskStateModelFactory(new Mocks.MockManager(), taskFactoryMap);

    List<TaskStateModel> taskStateModelList = Lists.newArrayList();
    for (int i = 0; i < 3; i++) {
      ResourceId resourceId = ResourceId.from("Task_" + i);
      taskStateModelList.add(
          taskStateModelFactory.createStateTransitionHandler(resourceId, PartitionId.from(resourceId, "")));
    }

    taskStateModelFactory.shutdown();

    for (TaskStateModel taskStateModel : taskStateModelList) {
      Assert.assertTrue(taskStateModel.isShutdown());
    }
  }

  private static class TestTaskFactory implements TaskFactory {

    @Override
    public Task createNewTask(TaskCallbackContext context) {
      return new TestTask();
    }

    private static class TestTask implements Task {

      @Override
      public TaskResult run() {
        return new TaskResult(TaskResult.Status.COMPLETED, "");
      }

      @Override
      public void cancel() {

      }
    }
  }
}
