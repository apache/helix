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
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.model.Message;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TargetState;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;

/**
 * Static test utility methods.
 */
public class TaskTestUtil {
  public static final String JOB_KW = "JOB";
  private final static int _default_timeout = 2 * 60 * 1000; /* 2 mins */

  public static void pollForEmptyJobState(final TaskDriver driver, final String workflowName,
      final String jobName) throws Exception {
    final String namespacedJobName = String.format("%s_%s", workflowName, jobName);
    boolean succeed = TestHelper.verify(new TestHelper.Verifier() {

      @Override public boolean verify() throws Exception {
        WorkflowContext ctx = driver.getWorkflowContext(workflowName);
        return ctx == null || ctx.getJobState(namespacedJobName) == null;
      }
    }, _default_timeout);
    Assert.assertTrue(succeed);
  }

  public static WorkflowContext pollForWorkflowContext(TaskDriver driver, String workflowResource)
      throws InterruptedException {
    // Wait for completion.
    long st = System.currentTimeMillis();
    WorkflowContext ctx;
    do {
      ctx = driver.getWorkflowContext(workflowResource);
      Thread.sleep(100);
    } while (ctx == null && System.currentTimeMillis() < st + _default_timeout);
    Assert.assertNotNull(ctx);
    return ctx;
  }

  // 1. Different jobs in a same work flow is in RUNNING at the same time
  // 2. When disallow overlap assignment, no two jobs in the same work flow is in RUNNING at the same instance
  // Use this method with caution because it assumes workflow doesn't finish too quickly and number of parallel running
  // tasks can be counted.
  public static boolean pollForWorkflowParallelState(TaskDriver driver, String workflowName)
      throws InterruptedException {

    WorkflowConfig workflowConfig = driver.getWorkflowConfig(workflowName);
    Assert.assertNotNull(workflowConfig);

    WorkflowContext workflowContext = null;
    while (workflowContext == null) {
      workflowContext = driver.getWorkflowContext(workflowName);
      Thread.sleep(100);
    }

    int maxRunningCount = 0;
    boolean finished = false;

    while (!finished) {
      finished = true;
      int runningCount = 0;

      workflowContext = driver.getWorkflowContext(workflowName);
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        TaskState jobState = workflowContext.getJobState(jobName);
        if (jobState == TaskState.IN_PROGRESS) {
          ++runningCount;
          finished = false;
        }
      }

      if (runningCount > maxRunningCount ) {
        maxRunningCount = runningCount;
      }

      List<JobContext> jobContextList = new ArrayList<JobContext>();
      for (String jobName : workflowConfig.getJobDag().getAllNodes()) {
        JobContext jobContext = driver.getJobContext(jobName);
        if (jobContext != null) {
          jobContextList.add(driver.getJobContext(jobName));
        }
      }

      if (!workflowConfig.isAllowOverlapJobAssignment()) {
        Set<String> instances = new HashSet<String>();
        for (JobContext jobContext : jobContextList) {
          for (int partition : jobContext.getPartitionSet()) {
            String instance = jobContext.getAssignedParticipant(partition);
            TaskPartitionState taskPartitionState = jobContext.getPartitionState(partition);

            if (instance == null) {
              continue;
            }
            if (taskPartitionState != TaskPartitionState.INIT && taskPartitionState != TaskPartitionState.RUNNING) {
              continue;
            }
            if (instances.contains(instance)) {
              return false;
            }

            TaskPartitionState state = jobContext.getPartitionState(partition);
            if (state != TaskPartitionState.COMPLETED) {
              instances.add(instance);
            }
          }
        }
      }

      Thread.sleep(100);
    }

    return maxRunningCount > 1 && (workflowConfig.isJobQueue() ? maxRunningCount <= workflowConfig
        .getParallelJobs() : true);
  }

  public static Date getDateFromStartTime(String startTime)
  {
    int splitIndex = startTime.indexOf(':');
    int hourOfDay = 0, minutes = 0;
    try
    {
      hourOfDay = Integer.parseInt(startTime.substring(0, splitIndex));
      minutes = Integer.parseInt(startTime.substring(splitIndex + 1));
    }
    catch (NumberFormatException e)
    {

    }
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.HOUR_OF_DAY, hourOfDay);
    cal.set(Calendar.MINUTE, minutes);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    return cal.getTime();
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart) {
    return buildRecurrentJobQueue(jobQueueName, delayStart, 60);
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart,
      int recurrenInSeconds) {
    return buildRecurrentJobQueue(jobQueueName, delayStart, recurrenInSeconds, null);
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart,
      int recurrenInSeconds, TargetState targetState) {
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder(jobQueueName);
    workflowCfgBuilder.setExpiry(120000);
    if (targetState != null) {
      workflowCfgBuilder.setTargetState(TargetState.STOP);
    }

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    ScheduleConfig scheduleConfig =
        ScheduleConfig.recurringFromDate(cal.getTime(), TimeUnit.SECONDS, recurrenInSeconds);
    workflowCfgBuilder.setScheduleConfig(scheduleConfig);
    return new JobQueue.Builder(jobQueueName).setWorkflowConfig(workflowCfgBuilder.build());
  }

  public static JobQueue.Builder buildRecurrentJobQueue(String jobQueueName) {
    return buildRecurrentJobQueue(jobQueueName, 0);
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName, int delayStart,
      int failureThreshold, int capacity) {
    WorkflowConfig.Builder workflowCfgBuilder = new WorkflowConfig.Builder(jobQueueName);
    workflowCfgBuilder.setExpiry(120000);
    workflowCfgBuilder.setCapacity(capacity);

    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    workflowCfgBuilder.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(cal.getTime()));

    if (failureThreshold > 0) {
      workflowCfgBuilder.setFailureThreshold(failureThreshold);
    }
    return new JobQueue.Builder(jobQueueName).setWorkflowConfig(workflowCfgBuilder.build());
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName, int delayStart,
      int failureThreshold) {
    return buildJobQueue(jobQueueName, delayStart, failureThreshold, 500);
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName) {
    return buildJobQueue(jobQueueName, 0, 0, 500);
  }

  public static JobQueue.Builder buildJobQueue(String jobQueueName, int capacity) {
    return buildJobQueue(jobQueueName, 0, 0, capacity);
  }

  public static WorkflowContext buildWorkflowContext(String workflowResource,
      TaskState workflowState, Long startTime, TaskState... jobStates) {
    WorkflowContext workflowContext =
        new WorkflowContext(new ZNRecord(TaskUtil.WORKFLOW_CONTEXT_KW));
    workflowContext.setName(workflowResource);
    workflowContext.setStartTime(startTime == null ? System.currentTimeMillis() : startTime);
    int jobId = 0;
    for (TaskState jobstate : jobStates) {
      workflowContext
          .setJobState(TaskUtil.getNamespacedJobName(workflowResource, JOB_KW) + jobId++, jobstate);
    }
    workflowContext.setWorkflowState(workflowState);
    return workflowContext;
  }

  public static JobContext buildJobContext(Long startTime, Long finishTime, TaskPartitionState... partitionStates) {
    JobContext jobContext = new JobContext(new ZNRecord(TaskUtil.TASK_CONTEXT_KW));
    jobContext.setStartTime(startTime == null ? System.currentTimeMillis() : startTime);
    jobContext.setFinishTime(finishTime == null ? System.currentTimeMillis() : finishTime);
    int partitionId = 0;
    for (TaskPartitionState partitionState : partitionStates) {
      jobContext.setPartitionState(partitionId++, partitionState);
    }
    return jobContext;
  }

  public static ClusterDataCache buildClusterDataCache(HelixDataAccessor accessor,
      String clusterName) {
    ClusterDataCache cache = new ClusterDataCache(clusterName);
    cache.refresh(accessor);
    cache.setTaskCache(true);
    return cache;
  }

  static void runStage(ClusterEvent event, Stage stage) throws Exception {
    StageContext context = new StageContext();
    stage.init(context);
    stage.preProcess();
    stage.process(event);
    stage.postProcess();
  }

  public static BestPossibleStateOutput calculateBestPossibleState(ClusterDataCache cache,
      HelixManager manager) throws Exception {
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    List<Stage> stages = new ArrayList<Stage>();
    stages.add(new ReadClusterDataStage());
    stages.add(new ResourceComputationStage());
    stages.add(new CurrentStateComputationStage());
    stages.add(new BestPossibleStateCalcStage());

    for (Stage stage : stages) {
      runStage(event, stage);
    }

    return event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
  }

  public static boolean pollForAllTasksBlock(HelixDataAccessor accessor, String instance, int numTask, long timeout)
      throws InterruptedException {
    PropertyKey propertyKey = accessor.keyBuilder().messages(instance);

    long startTime = System.currentTimeMillis();
    while (true) {
      List<Message> messages = accessor.getChildValues(propertyKey);
      if (allTasksBlock(messages, numTask)) {
        return true;
      } else if (startTime + timeout < System.currentTimeMillis()) {
        return false;
      } else {
        Thread.sleep(100);
      }
    }
  }

  private static boolean allTasksBlock(List<Message> messages, int numTask) {
    if (messages.size() != numTask) {
      return false;
    }
    for (Message message : messages) {
      if (!message.getFromState().equals(TaskPartitionState.INIT.name())
          || !message.getToState().equals(TaskPartitionState.RUNNING.name())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Implement this class to periodically check whether a defined condition is true,
   * if timeout, check the condition for the last time and return the result.
   */
  public static abstract class Poller {
    private static final long DEFAULT_TIME_OUT = 1000*10;

    public boolean poll() {
      return poll(DEFAULT_TIME_OUT);
    }

    public boolean poll(long timeOut) {
      long startTime = System.currentTimeMillis();
      while (System.currentTimeMillis() < startTime + timeOut) {
        if (check()) {
          break;
        }
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new IllegalStateException(e);
        }
      }
      return check();
    }

    public abstract boolean check();
  }
}
