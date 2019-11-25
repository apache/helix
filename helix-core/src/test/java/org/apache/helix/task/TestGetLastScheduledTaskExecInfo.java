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
import org.apache.helix.TestHelper;

public class TestGetLastScheduledTaskExecInfo extends TaskTestBase {
  private final static String TASK_START_TIME_KEY = "START_TIME";
  private final static long INVALID_TIMESTAMP = -1L;
  private static int SHORT_EXECUTION_TIME = 10;
  private static int LONG_EXECUTION_TIME = 99999999;
  private static final int DELETE_DELAY = 30 * 1000;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
  }

  @Test
  public void testGetLastScheduledTaskExecInfo() throws Exception {
    // Start new queue that has one job with long tasks and record start time of the tasks
    String queueName = TestHelper.getTestMethodName();
    // Create and start new queue that has one job with 5 tasks.
    // Each task has a long execution time.
    // Since NumConcurrentTasksPerInstance is equal to 2, here we wait until two tasks have
    // been scheduled (expectedScheduledTime = 2).
    List<Long> startTimesWithStuckTasks = setupTasks(queueName, 5, LONG_EXECUTION_TIME, 2);
    // Wait till the job is in progress
    _driver.pollForJobState(queueName, queueName + "_job_0", TaskState.IN_PROGRESS);

    // First two must be -1 (two tasks are stuck), and API call must return the last value (most
    // recent timestamp)
    Assert.assertEquals(startTimesWithStuckTasks.get(0).longValue(), INVALID_TIMESTAMP);
    Assert.assertEquals(startTimesWithStuckTasks.get(1).longValue(), INVALID_TIMESTAMP);

    // Workflow will be stuck so its partition state will be Running
    boolean hasQueueReachedDesiredState = TestHelper.verify(() -> {
      Long lastScheduledTaskTs = _driver.getLastScheduledTaskTimestamp(queueName);
      TaskExecutionInfo execInfo = _driver.getLastScheduledTaskExecutionInfo(queueName);
      return (execInfo.getJobName().equals(queueName + "_job_0")
          && execInfo.getTaskPartitionState() == TaskPartitionState.RUNNING
          && execInfo.getStartTimeStamp().equals(lastScheduledTaskTs)
          && startTimesWithStuckTasks.get(4).equals(lastScheduledTaskTs));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(hasQueueReachedDesiredState);

    // Stop and delete the queue
    _driver.stop(queueName);
    _driver.deleteAndWaitForCompletion(queueName, DELETE_DELAY);

    // Start the new queue with new task configuration.
    // Create and start new queue that has one job with 4 tasks.
    // Each task has a short execution time. In the setupTasks we wait until all of the tasks have
    // been scheduled (expectedScheduledTime = 4).
    List<Long> startTimesFastTasks = setupTasks(queueName, 4, SHORT_EXECUTION_TIME, 4);
    // Wait till the job is in progress or completed. Since the tasks have short execution time, we
    // wait for either IN_PROGRESS or COMPLETED states
    _driver.pollForJobState(queueName, queueName + "_job_0", TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    hasQueueReachedDesiredState = TestHelper.verify(() -> {
      Long lastScheduledTaskTs = _driver.getLastScheduledTaskTimestamp(queueName);
      TaskExecutionInfo execInfo = _driver.getLastScheduledTaskExecutionInfo(queueName);
      return (execInfo.getJobName().equals(queueName + "_job_0")
          && execInfo.getTaskPartitionState() == TaskPartitionState.COMPLETED
          && execInfo.getStartTimeStamp().equals(lastScheduledTaskTs)
          && startTimesFastTasks.get(startTimesFastTasks.size() - 1).equals(lastScheduledTaskTs));
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(hasQueueReachedDesiredState);
  }

  /**
   * Helper method for gathering start times for all tasks. Returns start times in ascending order.
   * Null start times
   * are recorded as 0.
   * @param jobQueueName name of the queue
   * @param numTasks number of tasks to schedule
   * @param taskTimeout duration of each task to be run for
   * @param expectedScheduledTasks expected number of tasks that should be scheduled
   * @return list of timestamps for all tasks in ascending order
   * @throws Exception
   */
  private List<Long> setupTasks(String jobQueueName, int numTasks, long taskTimeout,
      int expectedScheduledTasks) throws Exception {
    // Create a queue
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(jobQueueName);

    // Create and enqueue a job
    JobConfig.Builder jobConfig = new JobConfig.Builder();

    // Create tasks
    List<TaskConfig> taskConfigs = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      taskConfigs
          .add(new TaskConfig.Builder().setTaskId("task_" + i).setCommand(MockTask.TASK_COMMAND)
              .addConfig(MockTask.JOB_DELAY, String.valueOf(taskTimeout)).build());
    }
    // Run up to 2 tasks at a time
    jobConfig.addTaskConfigs(taskConfigs).setNumConcurrentTasksPerInstance(2);
    queueBuilder.enqueueJob("job_0", jobConfig);
    _driver.start(queueBuilder.build());

    _driver.pollForWorkflowState(jobQueueName, TaskState.IN_PROGRESS);

    boolean haveExpectedNumberOfTasksScheduled = TestHelper.verify(() -> {
      int scheduleTask = 0;
      WorkflowConfig workflowConfig =
          TaskUtil.getWorkflowConfig(_manager.getHelixDataAccessor(), jobQueueName);
      for (String job : workflowConfig.getJobDag().getAllNodes()) {
        JobContext jobContext = _driver.getJobContext(job);
        Set<Integer> allPartitions = jobContext.getPartitionSet();
        for (Integer partition : allPartitions) {
          String timestamp = jobContext.getMapField(partition).get(TASK_START_TIME_KEY);
          if (timestamp != null) {
            scheduleTask++;
          }
        }
      }
      return (scheduleTask == expectedScheduledTasks);
    }, TestHelper.WAIT_DURATION);
    Assert.assertTrue(haveExpectedNumberOfTasksScheduled);

    // Pull jobContexts and look at the start times
    List<Long> startTimes = new ArrayList<>();
    WorkflowConfig workflowConfig =
        TaskUtil.getWorkflowConfig(_manager.getHelixDataAccessor(), jobQueueName);
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
