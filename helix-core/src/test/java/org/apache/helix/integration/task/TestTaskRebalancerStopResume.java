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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobDag;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

public class TestTaskRebalancerStopResume extends ZkTestBase {
  private static final Logger LOG = Logger.getLogger(TestTaskRebalancerStopResume.class);
  private static final int n = 5;
  private static final int START_PORT = 12918;
  private static final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private static final String TIMEOUT_CONFIG = "Timeout";
  private static final String TGT_DB = "TestDB";
  private static final String JOB_RESOURCE = "SomeJob";
  private static final int NUM_PARTITIONS = 20;
  private static final int NUM_REPLICAS = 3;
  private final String CLUSTER_NAME = "TestTaskRebalancerStopResume";
  private final MockParticipant[] _participants = new MockParticipant[n];
  private MockController _controller;

  private HelixManager _manager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println("START " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }

    _setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < n; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // Set up target db
    _setupTool.addResourceToCluster(CLUSTER_NAME, TGT_DB, NUM_PARTITIONS, MASTER_SLAVE_STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TGT_DB, NUM_REPLICAS);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("Reindex", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new ReindexTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      _participants[i] = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory(StateModelDefId.from("Task"),
          new TaskStateModelFactory(_participants[i], taskFactoryReg));

      _participants[i].syncStart();
    }

    // start controller
    String controllerName = "controller_0";
    _controller = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // create cluster manager
    _manager =
        HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR,
            _zkaddr);
    _manager.connect();

    _driver = new TaskDriver(_manager);

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new ClusterStateVerifier.MasterNbInExtViewVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                CLUSTER_NAME));
    Assert.assertTrue(result);
    System.out.println("END BEFORECLASS " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @AfterClass
  public void afterClass() throws Exception {
    System.out.println("START AFTERCLASS " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
    _controller.syncStop();
    for (int i = 0; i < n; i++) {
      _participants[i].syncStop();
    }
    _manager.disconnect();
    System.out.println("END " + CLUSTER_NAME + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void stopAndResume() throws Exception {
    Map<String, String> commandConfig = ImmutableMap.of(TIMEOUT_CONFIG, String.valueOf(100));
    Workflow flow =
        WorkflowGenerator.generateDefaultSingleJobWorkflowBuilderWithExtraConfigs(JOB_RESOURCE,
            commandConfig).build();

    LOG.info("Starting flow " + flow.getName());
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, JOB_RESOURCE, TaskState.IN_PROGRESS);

    LOG.info("Pausing job");
    _driver.stop(JOB_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, JOB_RESOURCE, TaskState.STOPPED);

    LOG.info("Resuming job");
    _driver.resume(JOB_RESOURCE);
    TestUtil.pollForWorkflowState(_manager, JOB_RESOURCE, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeWorkflow() throws Exception {
    String workflow = "SomeWorkflow";
    Workflow flow = WorkflowGenerator.generateDefaultRepeatedJobWorkflowBuilder(workflow).build();

    LOG.info("Starting flow " + workflow);
    _driver.start(flow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.IN_PROGRESS);

    LOG.info("Pausing workflow");
    _driver.stop(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.STOPPED);

    LOG.info("Resuming workflow");
    _driver.resume(workflow);
    TestUtil.pollForWorkflowState(_manager, workflow, TaskState.COMPLETED);
  }

  @Test
  public void stopAndResumeNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Enqueue jobs
    Set<String> master = Sets.newHashSet("MASTER");
    JobConfig.Builder job1 =
        new JobConfig.Builder().setCommand("Reindex")
            .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(master);
    String job1Name = "masterJob";
    LOG.info("Enqueuing job: " + job1Name);
    _driver.enqueueJob(queueName, job1Name, job1);

    Set<String> slave = Sets.newHashSet("SLAVE");
    JobConfig.Builder job2 =
    new JobConfig.Builder().setCommand("Reindex")
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB).setTargetPartitionStates(slave);
    String job2Name = "slaveJob";
    LOG.info("Enqueuing job: " + job2Name);
    _driver.enqueueJob(queueName, job2Name, job2);

    String namespacedJob1 = String.format("%s_%s", queueName,  job1Name);
    TestUtil.pollForJobState(_manager, queueName, namespacedJob1, TaskState.IN_PROGRESS);

    // stop job1
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    TestUtil.pollForJobState(_manager, queueName, namespacedJob1, TaskState.STOPPED);
    TestUtil.pollForWorkflowState(_manager, queueName, TaskState.STOPPED);

    // Ensure job2 is not started
    TimeUnit.MILLISECONDS.sleep(200);
    String namespacedJob2 = String.format("%s_%s", queueName, job2Name);
    TestUtil.pollForEmptyJobState(_manager, queueName, job2Name);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // Ensure successful completion
    TestUtil.pollForJobState(_manager, queueName, namespacedJob1, TaskState.COMPLETED);
    TestUtil.pollForJobState(_manager, queueName, namespacedJob2, TaskState.COMPLETED);
    JobContext masterJobContext = TaskUtil.getJobContext(_manager, namespacedJob1);
    JobContext slaveJobContext = TaskUtil.getJobContext(_manager, namespacedJob2);

    // Ensure correct ordering
    long job1Finish = masterJobContext.getFinishTime();
    long job2Start = slaveJobContext.getStartTime();
    Assert.assertTrue(job2Start >= job1Finish);

    // Flush queue and check cleanup
    LOG.info("Flusing job-queue: " + queueName);
    _driver.flushQueue(queueName);

    verifyJobDeleted(queueName, namespacedJob1);
    verifyJobDeleted(queueName, namespacedJob2);
    verifyJobNotInQueue(queueName, namespacedJob1);
    verifyJobNotInQueue(queueName, namespacedJob2);
  }

  @Test
  public void stopDeleteAndResumeNamedQueue() throws Exception {
    String queueName = TestHelper.getTestMethodName();

    // Create a queue
    LOG.info("Starting job-queue: " + queueName);
    JobQueue queue = new JobQueue.Builder(queueName).build();
    _driver.createQueue(queue);

    // Create and Enqueue jobs
    List<String> currentJobNames = new ArrayList<String>();
    for (int i = 0; i <= 4; i++) {
      String targetPartition = (i == 0) ? "MASTER" : "SLAVE";

      JobConfig.Builder job =
          new JobConfig.Builder().setCommand("Reindex")
              .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB)
              .setTargetPartitionStates(Sets.newHashSet(targetPartition));
      String jobName = targetPartition.toLowerCase() + "Job" + i;
      LOG.info("Enqueuing job: " + jobName);
      _driver.enqueueJob(queueName, jobName, job);
      currentJobNames.add(i, jobName);
    }

    // ensure job 1 is started before deleting it
    String deletedJob1 = currentJobNames.get(0);
    String namedSpaceDeletedJob1 = String.format("%s_%s", queueName, deletedJob1);
    TestUtil.pollForJobState(_manager, queueName, namedSpaceDeletedJob1, TaskState.IN_PROGRESS);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    TestUtil.pollForJobState(_manager, queueName, namedSpaceDeletedJob1, TaskState.STOPPED);
    TestUtil.pollForWorkflowState(_manager, queueName, TaskState.STOPPED);

    // delete the in-progress job (job 1) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob1);
    verifyJobDeleted(queueName, namedSpaceDeletedJob1);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // ensure job 2 is started
    TestUtil.pollForJobState(_manager, queueName,
        String.format("%s_%s", queueName, currentJobNames.get(1)), TaskState.IN_PROGRESS);

    // stop the queue
    LOG.info("Pausing job-queue: " + queueName);
    _driver.stop(queueName);
    TestUtil.pollForJobState(_manager, queueName,
        String.format("%s_%s", queueName, currentJobNames.get(1)), TaskState.STOPPED);
    TestUtil.pollForWorkflowState(_manager, queueName, TaskState.STOPPED);

    // Ensure job 3 is not started before deleting it
    String deletedJob2 = currentJobNames.get(2);
    String namedSpaceDeletedJob2 = String.format("%s_%s", queueName, deletedJob2);
    TestUtil.pollForEmptyJobState(_manager, queueName, namedSpaceDeletedJob2);

    // delete not-started job (job 3) and verify it being deleted
    _driver.deleteJob(queueName, deletedJob2);
    verifyJobDeleted(queueName, namedSpaceDeletedJob2);

    LOG.info("Resuming job-queue: " + queueName);
    _driver.resume(queueName);

    // Ensure the jobs left are successful completed in the correct order
    currentJobNames.remove(deletedJob1);
    currentJobNames.remove(deletedJob2);
    long preJobFinish = 0;
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", queueName, currentJobNames.get(i));
      TestUtil.pollForJobState(_manager, queueName, namedSpaceJobName, TaskState.COMPLETED);

      JobContext jobContext = TaskUtil.getJobContext(_manager, namedSpaceJobName);
      long jobStart = jobContext.getStartTime();
      Assert.assertTrue(jobStart >= preJobFinish);
      preJobFinish = jobContext.getFinishTime();
    }

    // Flush queue
    LOG.info("Flusing job-queue: " + queueName);
    _driver.flushQueue(queueName);

    TimeUnit.MILLISECONDS.sleep(200);
    // verify the cleanup
    for (int i = 0; i < currentJobNames.size(); i++) {
      String namedSpaceJobName = String.format("%s_%s", queueName, currentJobNames.get(i));
      verifyJobDeleted(queueName, namedSpaceJobName);
      verifyJobNotInQueue(queueName, namedSpaceJobName);
    }
  }

  private void verifyJobDeleted(String queueName, String jobName) throws Exception {
    HelixDataAccessor accessor = _manager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    Assert.assertNull(accessor.getProperty(keyBuilder.idealStates(jobName)));
    Assert.assertNull(accessor.getProperty(keyBuilder.resourceConfig(jobName)));
    TestUtil.pollForEmptyJobState(_manager, queueName, jobName);
  }

  private void verifyJobNotInQueue(String queueName, String namedSpacedJobName) {
    WorkflowConfig workflowCfg = TaskUtil.getWorkflowCfg(_manager, queueName);
    JobDag dag = workflowCfg.getJobDag();
    Assert.assertFalse(dag.getAllNodes().contains(namedSpacedJobName));
    Assert.assertFalse(dag.getChildrenToParents().containsKey(namedSpacedJobName));
    Assert.assertFalse(dag.getParentsToChildren().containsKey(namedSpacedJobName));
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
        sleep(50);
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
