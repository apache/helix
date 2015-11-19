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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.ZkIntegrationTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.task.WorkflowContext;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestRecurringJobQueue extends ZkIntegrationTestBase {
  private static final Logger LOG = Logger.getLogger(TestRecurringJobQueue.class);
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final String TIMEOUT_CONFIG = "Timeout";
  private static final String TGT_DB = "TestDB";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private final MockParticipantManager[] _participants = new MockParticipantManager[n];
  private ClusterControllerManager _controller;

  private HelixManager _manager;
  private TaskDriver _driver;
  private ZKHelixDataAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    setupTool.addResourceToCluster(CLUSTER_NAME, TGT_DB, NUM_PARTITIONS, MASTER_SLAVE_STATE_MODEL);
    setupTool.rebalanceStorageCluster(CLUSTER_NAME, TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new ReindexTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task", new TaskStateModelFactory(_participants[i],
          taskFactoryReg));

      _participants[i].syncStart();
    }

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            ZK_ADDR);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.MasterNbInExtViewVerifier(
            ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _controller.syncStop();
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
    }
    _manager.disconnect();
  }

  private Date getDateFromStartTime(String startTime)
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

  private JobQueue.Builder buildRecurrentJobQueue(String jobQueueName, int delayStart) {
    Map<String, String> cfgMap = new HashMap<String, String>();
    cfgMap.put(WorkflowConfig.EXPIRY, String.valueOf(120000));
    cfgMap.put(WorkflowConfig.RECURRENCE_INTERVAL, String.valueOf(60));
    cfgMap.put(WorkflowConfig.RECURRENCE_UNIT, "SECONDS");
    Calendar cal = Calendar.getInstance();
    cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + delayStart / 60);
    cal.set(Calendar.SECOND, cal.get(Calendar.SECOND) + delayStart % 60);
    cal.set(Calendar.MILLISECOND, 0);
    cfgMap.put(WorkflowConfig.START_TIME,
        WorkflowConfig.getDefaultDateFormat().format(cal.getTime()));
    //cfgMap.put(WorkflowConfig.START_TIME,
    //WorkflowConfig.getDefaultDateFormat().format(getDateFromStartTime("00:00")));
    return new JobQueue.Builder(jobQueueName).fromMap(cfgMap);
  }


  private JobQueue.Builder buildRecurrentJobQueue(String jobQueueName) {
    return buildRecurrentJobQueue(jobQueueName, 0);
  }

  @Test
  public void deleteRecreateRecurrentQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuild = buildRecurrentJobQueue(queueName);
    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i <= 1; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("Reindex")
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuild.enqueueJob(jobName, job);
      currentJobNames.add(jobName);
    }

    _driver.start(queueBuild.build());

    WorkflowContext wCtx = TestUtil.pollForWorkflowContext(_manager, queueName);

    // ensure job 1 is started before stop it
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    String namedSpaceJob1 = String.format("%s_%s", scheduledQueue, currentJobNames.get(0));
    TestUtil
        .pollForJobState(_manager, scheduledQueue, namedSpaceJob1, TaskState.IN_PROGRESS);

    _driver.stop(queueName);
    _driver.delete(queueName);
    Thread.sleep(500);

    JobQueue.Builder queueBuilder = buildRecurrentJobQueue(queueName, 5);
    currentJobNames.clear();
    for (int i = 0; i <= 1; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("Reindex")
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      queueBuilder.enqueueJob(jobName, job);
      currentJobNames.add(jobName);
    }

    _driver.createQueue(queueBuilder.build());


    wCtx = TestUtil.pollForWorkflowContext(_manager, queueName);

    // ensure jobs are started and completed
    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    namedSpaceJob1 = String.format("%s_%s", scheduledQueue, currentJobNames.get(0));
    TestUtil
        .pollForJobState(_manager, scheduledQueue, namedSpaceJob1, TaskState.COMPLETED);

    scheduledQueue = wCtx.getLastScheduledSingleWorkflow();
    String namedSpaceJob2 = String.format("%s_%s", scheduledQueue, currentJobNames.get(1));
    TestUtil
        .pollForJobState(_manager, scheduledQueue, namedSpaceJob2, TaskState.COMPLETED);
  }

  @Test
  public void stopDeleteJobAndResumeRecurrentQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildRecurrentJobQueue(queueName, 5);

    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(500));
    Thread.sleep(100);
    for (int i = 0; i <= 4; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("Reindex")
              .setJobCommandConfigMap(commandConfig)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      LOG.info("Enqueuing job: " + jobName);
      queueBuilder.enqueueJob(jobName, job);
      currentJobNames.add(i, jobName);
    }
    _driver.createQueue(queueBuilder.build());

    WorkflowContext wCtx = TestUtil.pollForWorkflowContext(_manager, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure job 1 is started before deleting it
    String deletedJob1 = currentJobNames.get(0);
    String namedSpaceDeletedJob1 = String.format("%s_%s", scheduledQueue, deletedJob1);
    TestUtil
        .pollForJobState(_manager, scheduledQueue, namedSpaceDeletedJob1, TaskState.IN_PROGRESS,
            TaskState.COMPLETED);

    // stop the queue
    LOG.info("Pausing job-queue: " + scheduledQueue);
    _driver.stop(queueName);
    TestUtil.pollForJobState(_manager, scheduledQueue, namedSpaceDeletedJob1, TaskState.STOPPED);
    TestUtil.pollForWorkflowState(_manager, scheduledQueue, TaskState.STOPPED);

    // delete the in-progress job (job 1) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob1);
    verifyJobDeleted(queueName, namedSpaceDeletedJob1);
    verifyJobDeleted(scheduledQueue, namedSpaceDeletedJob1);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // ensure job 2 is started
    TestUtil.pollForJobState(_manager, scheduledQueue,
        String.format("%s_%s", scheduledQueue, currentJobNames.get(1)), TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    TestUtil.pollForJobState(_manager, scheduledQueue,
        String.format("%s_%s", scheduledQueue, currentJobNames.get(1)), TaskState.STOPPED);
    TestUtil.pollForWorkflowState(_manager, scheduledQueue, TaskState.STOPPED);

    // Ensure job 3 is not started before deleting it
    String deletedJob2 = currentJobNames.get(2);
    String namedSpaceDeletedJob2 = String.format("%s_%s", scheduledQueue, deletedJob2);
    TestUtil.pollForEmptyJobState(_manager, scheduledQueue, namedSpaceDeletedJob2);

    // delete not-started job (job 3) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob2);
    verifyJobDeleted(queueName, namedSpaceDeletedJob2);
    verifyJobDeleted(scheduledQueue, namedSpaceDeletedJob2);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // Ensure the jobs left are successful completed in the correct order
    currentJobNames.remove(deletedJob1);
    currentJobNames.remove(deletedJob2);
    long preJobFinish = 0;
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", scheduledQueue, currentJobNames.get(i));
      TestUtil.pollForJobState(_manager, scheduledQueue, namedSpaceJobName, TaskState.COMPLETED);

      JobContext jobContext = TaskUtil.getJobContext(_manager, namedSpaceJobName);
      long jobStart = jobContext.getStartTime();
      Assert.assertTrue(jobStart >= preJobFinish);
      preJobFinish = jobContext.getFinishTime();
    }
    // verify the job is not there for the next recurrence of queue schedule
  }

  @Test
  public void deleteJobFromRecurrentQueueNotStarted() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildRecurrentJobQueue(queueName);

    // create jobs
    List<JobConfig.Builder> jobs = new ArrayList<JobConfig.Builder>();
    List<String> jobNames = new ArrayList<String>();
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(500));

    final int JOB_COUNTS = 3;
    for (int i = 0; i < JOB_COUNTS; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("Reindex").setJobCommandConfigMap(commandConfig)
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      jobs.add(job);
      jobNames.add(targetPartition.toLowerCase() + "Job" + i);
    }

    // enqueue all jobs except last one
    for (int i = 0; i < JOB_COUNTS - 1; ++i) {
      LOG.info("Enqueuing job: " + jobNames.get(i));
      queueBuilder.enqueueJob(jobNames.get(i), jobs.get(i));
    }

    _driver.createQueue(queueBuilder.build());

    String currentLastJob = jobNames.get(JOB_COUNTS - 2);

    WorkflowContext wCtx = TestUtil.pollForWorkflowContext(_manager, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure all jobs are finished
    String namedSpaceJob = String.format("%s_%s", scheduledQueue, currentLastJob);
    TestUtil.pollForJobState(_manager, scheduledQueue, namedSpaceJob, TaskState.COMPLETED);

    // enqueue the last job
    LOG.info("Enqueuing job: " + jobNames.get(JOB_COUNTS - 1));
    _driver.enqueueJob(queueName, jobNames.get(JOB_COUNTS - 1), jobs.get(JOB_COUNTS - 1));
    _driver.stop(queueName);

    // remove the last job
    _driver.deleteJob(queueName, jobNames.get(JOB_COUNTS - 1));

    // verify
    verifyJobDeleted(queueName,
        String.format("%s_%s", scheduledQueue, jobNames.get(JOB_COUNTS - 1)));
  }

  @Test
  public void testJobsDisableExternalView() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue.Builder queueBuilder = buildRecurrentJobQueue(queueName);

    // create jobs
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(500));

    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand("Reindex").setJobCommandConfigMap(commandConfig)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setTargetPartitionStates(Sets.newHashSet("SLAVE"))
            .setDisableExternalView(true);

    JobConfig.Builder job2 =
        new JobConfig.Builder().setCommand("Reindex").setJobCommandConfigMap(commandConfig)
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
            .setTargetPartitionStates(Sets.newHashSet("MASTER"));

    // enqueue both jobs
    queueBuilder.enqueueJob("job1", job1);
    queueBuilder.enqueueJob("job2", job2);

    _driver.createQueue(queueBuilder.build());


    WorkflowContext wCtx = TestUtil.pollForWorkflowContext(_manager, queueName);
    String scheduledQueue = wCtx.getLastScheduledSingleWorkflow();

    // ensure job1 is started
    String namedSpaceJob1 = String.format("%s_%s", scheduledQueue, "job1");
    TestUtil.pollForJobState(_manager, scheduledQueue, namedSpaceJob1, TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    PropertyKey.Builder keyBuilder = _accessor.keyBuilder();
    // verify external view for job does not exists
    ExternalView externalView = _accessor.getProperty(keyBuilder.externalView(namedSpaceJob1));
    Assert.assertNull(externalView, "External View for " + namedSpaceJob1 + " shoudld not exist!");

    // ensure job2 is completed
    String namedSpaceJob2 = String.format("%s_%s", scheduledQueue, "job2");
    TestUtil.pollForJobState(_manager, scheduledQueue, namedSpaceJob2, TaskState.IN_PROGRESS,
        TaskState.COMPLETED);

    // verify external view for job does not exists
    externalView = _accessor.getProperty(keyBuilder.externalView(namedSpaceJob2));
    Assert.assertNotNull(externalView, "Can not find external View for " + namedSpaceJob2 + "!");
  }


  private void verifyJobDeleted(String queueName, String jobName) throws Exception {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(jobName)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(jobName)));
    TestUtil.pollForEmptyJobState(_manager, queueName, jobName);
  }

  public static class ReindexTask implements Task {
    private final long _delay;
    private volatile boolean _canceled;

    public ReindexTask(TaskCallbackContext context) {
      JobConfig jobCfg = context.getJobConfig();
      Map<String, String> cfg = jobCfg.getJobCommandConfigMap();
      if (cfg == null) {
        cfg = Collections.emptyMap();
      }
      _delay = cfg.containsKey(TIMEOUT_CONFIG) ? Long.parseLong(cfg.get(TIMEOUT_CONFIG)) : 200L;
    }

    @Override
    public TaskResult run() {
      long expiry = System.currentTimeMillis() + _delay;
      long timeLeft;
      while (System.currentTimeMillis() < expiry) {
        if (_canceled) {
          timeLeft = expiry - System.currentTimeMillis();
          return new TaskResult(TaskResult.Status.CANCELED, String.valueOf(timeLeft < 0 ? 0
              : timeLeft));
        }
        sleep(10L);
      }
      timeLeft = expiry - System.currentTimeMillis();
      return new TaskResult(TaskResult.Status.COMPLETED,
          String.valueOf(timeLeft < 0 ? 0 : timeLeft));
    }

    @Override
    public void cancel() {
      _canceled = true;
    }

    private static void sleep(long d) {
      try {
        Thread.sleep(d);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}

