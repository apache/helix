package org.apache.helix.integration.task;

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

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.TestHelper;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TestGenericJobs extends TaskTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestGenericJobs.class);

  @Test public void testGenericJobs() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(queueName);

    // Create and Enqueue jobs
    int num_jobs = 4;
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i < num_jobs; i++) {
      JobConfig.Builder jobConfig = new JobConfig.Builder();

      // create each task configs.
      List<TaskConfig> taskConfigs = new ArrayList<TaskConfig>();
      int num_tasks = 10;
      for (int j = 0; j < num_tasks; j++) {
        taskConfigs.add(
            new TaskConfig.Builder().setTaskId("task_" + j).setCommand(MockTask.TASK_COMMAND)
                .build());
      }
      jobConfig.addTaskConfigs(taskConfigs);

      String jobName = "job_" + i;
      queueBuilder.enqueueJob(jobName, jobConfig);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuilder.build());

    String namedSpaceJob =
        String.format("%s_%s", queueName, currentJobNames.get(currentJobNames.size() - 1));
    _driver.pollForJobState(queueName, namedSpaceJob, TaskState.COMPLETED);
  }
}

