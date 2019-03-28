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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestGetLastScheduledTaskExecInfo extends TaskTestBase {
  private final static String TASK_START_TIME_KEY = "START_TIME";
  private final static long INVALID_TIMESTAMP = -1L;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testGetLastScheduledTaskExecInfo() throws InterruptedException {
    List<Long> startTimesWithStuckTasks = setupTasks("TestWorkflow_2", 5, 99999999);

    // First two must be -1 (two tasks are stuck), and API call must return the last value (most recent timestamp)
    Assert.assertEquals(startTimesWithStuckTasks.get(0).longValue(), INVALID_TIMESTAMP);
    Assert.assertEquals(startTimesWithStuckTasks.get(1).longValue(), INVALID_TIMESTAMP);
    TaskExecutionInfo execInfo = _driver.getLastScheduledTaskExecutionInfo("TestWorkflow_2");
    Long lastScheduledTaskTs = _driver.getLastScheduledTaskTimestamp("TestWorkflow_2");
    Assert.assertEquals(startTimesWithStuckTasks.get(3), lastScheduledTaskTs);

    Assert.assertEquals(execInfo.getJobName(), "TestWorkflow_2_job_0");
    // Workflow 2 will be stuck, so its partition state will be RUNNING
    Assert.assertEquals(execInfo.getTaskPartitionState(), TaskPartitionState.RUNNING);
    Assert.assertEquals(execInfo.getStartTimeStamp(), lastScheduledTaskTs);

    List<Long> startTimesFastTasks = setupTasks("TestWorkflow_3", 4, 10);
    // API call needs to return the most recent timestamp (value at last index)
    lastScheduledTaskTs = _driver.getLastScheduledTaskTimestamp("TestWorkflow_3");
    Thread.sleep(200); // Let the tasks run
    execInfo = _driver.getLastScheduledTaskExecutionInfo("TestWorkflow_3");

    Assert.assertEquals(startTimesFastTasks.get(startTimesFastTasks.size() - 1), lastScheduledTaskTs);
    Assert.assertEquals(execInfo.getJobName(), "TestWorkflow_3_job_0");
    Assert.assertEquals(execInfo.getTaskPartitionState(), TaskPartitionState.COMPLETED);
    Assert.assertEquals(execInfo.getStartTimeStamp(), lastScheduledTaskTs);
  }

  /**
   * Helper method for gathering start times for all tasks. Returns start times in ascending order. Null start times
   * are recorded as 0.
   *
   * @param jobQueueName name of the queue
   * @param numTasks number of tasks to schedule
   * @param taskTimeout duration of each task to be run for
   * @return list of timestamps for all tasks in ascending order
   * @throws InterruptedException
   */
  private List<Long> setupTasks(String jobQueueName, int numTasks, long taskTimeout) throws InterruptedException {
    // Create a queue
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(jobQueueName);

    // Create and enqueue a job
    JobConfig.Builder jobConfig = new JobConfig.Builder();

    // Create tasks
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      taskConfigs.add(new TaskConfig.Builder()
          .setTaskId("task_" + i)
          .setCommand(MockTask.TASK_COMMAND)
          .addConfig(MockTask.JOB_DELAY, String.valueOf(taskTimeout))
          .build());
    }
    // Run up to 2 tasks at a time
    jobConfig.addTaskConfigs(taskConfigs).setNumConcurrentTasksPerInstance(2);
    queueBuilder.enqueueJob("job_0", jobConfig);
    _driver.start(queueBuilder.build());
    // 1 second delay for the Controller
    Thread.sleep(1000);

    // Pull jobContexts and look at the start times
    List<Long> startTimes = new ArrayList<>();
    WorkflowConfig workflowConfig = TaskUtil.getWorkflowConfig(_manager.getHelixDataAccessor(), jobQueueName);
    for (String job : workflowConfig.getJobDag().getAllNodes()) {
      JobContext jobContext = _driver.getJobContext(job);
      Set<Integer> allPartitions = jobContext.getPartitionSet();
      for (Integer partition : allPartitions) {
        String timestamp = jobContext.getMapField(partition).get(TASK_START_TIME_KEY);
        if (timestamp == null) {
          startTimes.add(INVALID_TIMESTAMP);
        } else {
          startTimes.add(Long.parseLong(timestamp));
        }
      }
    }
    Collections.sort(startTimes);
    return startTimes;
  }
}