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

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestQuotaBasedScheduling extends TaskTestBase {
  private static final long LONG_RUNNING_TASK_DURATION = 100000L;
  private static final String DEFAULT_QUOTA_TYPE = "DEFAULT";
  private static final String JOB_COMMAND = "DummyCommand";
  private Map<String, String> _jobCommandMap;
  private Map<String, Integer> _quotaTypeExecutionCount = new ConcurrentHashMap<>();

  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2; // For easier debugging by inspecting ZNodes

    _participants = new MockParticipantManager[_numNodes];
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursively(namespace);
    }

    // Setup cluster and instances
    ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    setupTool.addCluster(CLUSTER_NAME, true);
    for (int i = 0; i < _numNodes; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      final String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);

      // Set task callbacks
      Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
      TaskFactory shortTaskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new ShortTask(context, instanceName);
        }
      };
      TaskFactory longTaskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new LongTask(context, instanceName);
        }
      };
      TaskFactory failTaskFactory = new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new FailTask(context, instanceName);
        }
      };
      taskFactoryReg.put("ShortTask", shortTaskFactory);
      taskFactoryReg.put("LongTask", longTaskFactory);
      taskFactoryReg.put("FailTask", failTaskFactory);

      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    // Start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Start an admin connection
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);

    _jobCommandMap = Maps.newHashMap();
  }

  @BeforeMethod
  public void beforeMethod() {
    _quotaTypeExecutionCount.clear();
  }

  /**
   * Tests whether jobs can run successfully without quotaTypes or quota configuration defined in
   * ClusterConfig. This test is to ensure backward-compatibility. This test must go first because
   * we want to make sure there is no quota config set anywhere.
   * @throws InterruptedException
   */
  @Test
  public void testSchedulingWithoutQuota() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    for (int i = 0; i < 10; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("ShortTask", new HashMap<String, String>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap);
      workflowBuilder.addJob("JOB" + i, jobConfigBulider);
    }

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    for (int i = 0; i < 10; i++) {
      String jobName = workflowName + "_" + "JOB" + i;
      TaskState jobState = _driver.getWorkflowContext(workflowName).getJobState(jobName);
      Assert.assertEquals(jobState, TaskState.COMPLETED);
    }
  }

  /**
   * Tests whether jobs with quotas can run successfully.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testSchedulingWithQuota() throws InterruptedException {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1);
    clusterConfig.setTaskQuotaRatio("A", 1);
    clusterConfig.setTaskQuotaRatio("B", 1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    for (int i = 0; i < 5; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("ShortTask", new HashMap<String, String>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap).setQuotaType("A");
      workflowBuilder.addJob("JOB" + i, jobConfigBulider);
    }

    for (int i = 5; i < 10; i++) {
      List<TaskConfig> taskConfigs = new ArrayList<>();
      taskConfigs.add(new TaskConfig("ShortTask", new HashMap<String, String>()));
      JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
          .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap).setQuotaType("B");
      workflowBuilder.addJob("JOB" + i, jobConfigBulider);
    }

    _driver.start(workflowBuilder.build());
    _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED);

    // Check job states
    for (int i = 0; i < 10; i++) {
      String jobName = workflowName + "_" + "JOB" + i;
      TaskState jobState = _driver.getWorkflowContext(workflowName).getJobState(jobName);
      Assert.assertEquals(jobState, TaskState.COMPLETED);
    }

    // Check run counts for each quota type
    Assert.assertEquals((int) _quotaTypeExecutionCount.get("A"), 5);
    Assert.assertEquals((int) _quotaTypeExecutionCount.get("B"), 5);
    Assert.assertFalse(_quotaTypeExecutionCount.containsKey(DEFAULT_QUOTA_TYPE));
  }

  /**
   * Tests that quota ratios are being observed. This is done by creating short tasks for some quota
   * types and long tasks for some quota types.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testSchedulingQuotaBottleneck() throws InterruptedException {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1);
    clusterConfig.setTaskQuotaRatio("A", 10); // Will get 19 threads
    clusterConfig.setTaskQuotaRatio("B", 10); // Will get 19 threads
    clusterConfig.setTaskQuotaRatio("C", 9); // Will get 1 thread
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create 3 jobs, 2 jobs of quotaType A and B with ShortTasks and 1 job of quotaType B with
    // LongTasks

    // JOB_A
    List<TaskConfig> taskConfigsA = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsA.add(new TaskConfig("ShortTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderA =
        new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigsA).setQuotaType("A").setNumConcurrentTasksPerInstance(20);
    workflowBuilder.addJob("JOB_A", jobBuilderA);

    // JOB_B
    List<TaskConfig> taskConfigsB = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsB.add(new TaskConfig("ShortTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderB =
        new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigsB).setQuotaType("B").setNumConcurrentTasksPerInstance(20);
    workflowBuilder.addJob("JOB_B", jobBuilderB);

    // JOB_C
    List<TaskConfig> taskConfigsC = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsC.add(new TaskConfig("LongTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderC =
        new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigsC).setQuotaType("C").setNumConcurrentTasksPerInstance(20);
    workflowBuilder.addJob("JOB_C", jobBuilderC);

    _driver.start(workflowBuilder.build());
    // Wait until JOB_A and JOB_B are done
    _driver.pollForJobState(workflowName, workflowName + "_JOB_A", TaskState.COMPLETED);
    _driver.pollForJobState(workflowName, workflowName + "_JOB_B", TaskState.COMPLETED);

    // At this point, JOB_C should still be in progress due to long-running tasks
    TaskState jobState =
        _driver.getWorkflowContext(workflowName).getJobState(workflowName + "_JOB_C");
    Assert.assertEquals(jobState, TaskState.IN_PROGRESS);
  }

  /**
   * Tests that in a single workflow, if there are multiple jobs with different quota types, one of
   * which is a long running quota type.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testWorkflowStuck() throws InterruptedException {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 10);
    clusterConfig.setTaskQuotaRatio("A", 10);
    clusterConfig.setTaskQuotaRatio("B", 10);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // JOB_A
    List<TaskConfig> taskConfigsA = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsA.add(new TaskConfig("LongTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderA =
        new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigsA).setQuotaType("A").setNumConcurrentTasksPerInstance(50);
    workflowBuilder.addJob("JOB_A", jobBuilderA);

    // JOB_B
    List<TaskConfig> taskConfigsB = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsB.add(new TaskConfig("LongTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderB =
        new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
            .addTaskConfigs(taskConfigsB).setQuotaType("B").setNumConcurrentTasksPerInstance(50);
    workflowBuilder.addJob("JOB_B", jobBuilderB);

    // JOB_C (DEFAULT type)
    List<TaskConfig> taskConfigsC = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsC.add(new TaskConfig("LongTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderC = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .setJobCommandConfigMap(_jobCommandMap).addTaskConfigs(taskConfigsC)
        .setQuotaType(DEFAULT_QUOTA_TYPE).setNumConcurrentTasksPerInstance(50);
    workflowBuilder.addJob("JOB_DEFAULT", jobBuilderC);

    _driver.start(workflowBuilder.build());
    // Wait until jobs are all in progress and saturated the thread pool
    _driver.pollForJobState(workflowName, workflowName + "_JOB_A", TaskState.IN_PROGRESS);
    _driver.pollForJobState(workflowName, workflowName + "_JOB_B", TaskState.IN_PROGRESS);
    _driver.pollForJobState(workflowName, workflowName + "_JOB_DEFAULT", TaskState.IN_PROGRESS);

    // Submit another workflow to make sure this doesn't run when the thread pool is saturated
    Workflow secondWorkflow =
        createWorkflow("secondWorkflow", true, DEFAULT_QUOTA_TYPE, 1, 1, "ShortTask");
    _driver.start(secondWorkflow);
    Thread.sleep(1000L); // Wait so that the Controller will try to process the workflow

    // At this point, secondWorkflow should still be in progress due to its task not being scheduled
    // due to thread pool saturation
    TaskState secondWorkflowState = _driver.getWorkflowContext("secondWorkflow").getWorkflowState();
    Assert.assertEquals(secondWorkflowState, TaskState.IN_PROGRESS);
  }

  /**
   * Tests that jobs belonging to a quota type that is not defined in ClusterConfig do not get
   * scheduled. That is, the job with an invalid quota type should never complete (because its tasks
   * may be assigned but never actually scheduled).
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testNotSchedulingInvalidQuotaType() throws InterruptedException {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1);
    clusterConfig.setTaskQuotaRatio("A", 19);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(true);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    // Create two jobs, JOB_A belonging to quotaType A and JOB_B to quotaType B (not defined)

    // JOB_A
    List<TaskConfig> taskConfigsA = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsA.add(new TaskConfig("ShortTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderA = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .setJobCommandConfigMap(_jobCommandMap).addTaskConfigs(taskConfigsA).setQuotaType("A");
    workflowBuilder.addJob("JOB_A", jobBuilderA);

    // JOB_B
    List<TaskConfig> taskConfigsB = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      Map<String, String> taskConfigMap = Maps.newHashMap();
      taskConfigsB.add(new TaskConfig("ShortTask", taskConfigMap));
    }
    JobConfig.Builder jobBuilderB = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .setJobCommandConfigMap(_jobCommandMap).addTaskConfigs(taskConfigsB).setQuotaType("B");
    workflowBuilder.addJob("JOB_B", jobBuilderB);

    _driver.start(workflowBuilder.build());
    // Wait until JOB_A is correctly scheduled and complete
    _driver.pollForJobState(workflowName, workflowName + "_JOB_A", TaskState.COMPLETED);

    // Check that JOB_B is still in progress and does not finish due to tasks not being scheduled
    TaskState jobState =
        _driver.getWorkflowContext(workflowName).getJobState(workflowName + "_JOB_B");
    Assert.assertEquals(jobState, TaskState.IN_PROGRESS);
  }

  /**
   * Tests that by repeatedly scheduling workflows and jobs that there is no thread leak when there
   * are a multidude of successful and failed tests. The number of total tasks run must be well
   * above the number of total thread capacity.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testThreadLeak() throws InterruptedException {
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1);
    clusterConfig.setTaskQuotaRatio("A", 1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    List<String> workflowNames = new ArrayList<>();

    // A word about these numbers. Currently, numNodes is 2, meaning each instance will have 40
    // threads, so we just need to make the total number of tasks well over 80
    int numWorkflows = 40;
    int numJobs = 3;
    int numTasks = 3;
    for (int i = 0; i < numWorkflows; i++) {
      boolean shouldOverlapJobAssign = i % 3 == 1; // Alternate between true and false
      String quotaType = (i % 2 == 1) ? null : "A"; // Alternate between null (DEFAULT) and A
      String taskType = (i % 3 == 1) ? "FailTask" : "ShortTask"; // Some tasks will fail
      // String taskType = "ShortTask";
      String workflowName = TestHelper.getTestMethodName() + "_" + i;
      workflowNames.add(workflowName); // For polling the state for these workflows

      Workflow workflow = createWorkflow(workflowName, shouldOverlapJobAssign, quotaType, numJobs,
          numTasks, taskType);
      _driver.start(workflow);
    }

    // Wait until all workflows finish
    for (String workflowName : workflowNames) {
      _driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.ABORTED,
          TaskState.TIMED_OUT, TaskState.FAILED);
    }

    for (int i = 0; i < numWorkflows; i++) {
      String workflowName = workflowNames.get(i);
      TaskState state = (i % 3 == 1) ? TaskState.FAILED : TaskState.COMPLETED;
      // TaskState state = TaskState.COMPLETED;
      Assert.assertEquals(_driver.getWorkflowContext(_manager, workflowName).getWorkflowState(),
          state);
    }
  }

  /**
   * Tests quota-based scheduling for a job queue with different quota types.
   * @throws InterruptedException
   */
  @Test(dependsOnMethods = "testSchedulingWithoutQuota")
  public void testJobQueueScheduling() throws InterruptedException {
    // First define quota config
    ClusterConfig clusterConfig = _manager.getConfigAccessor().getClusterConfig(CLUSTER_NAME);
    clusterConfig.resetTaskQuotaRatioMap();
    clusterConfig.setTaskQuotaRatio(DEFAULT_QUOTA_TYPE, 1);
    clusterConfig.setTaskQuotaRatio("A", 1);
    clusterConfig.setTaskQuotaRatio("B", 1);
    _manager.getConfigAccessor().setClusterConfig(CLUSTER_NAME, clusterConfig);

    String queueName = TestHelper.getTestMethodName();

    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder(queueName);
    workflowConfigBuilder.setParallelJobs(1);
    workflowConfigBuilder.setAllowOverlapJobAssignment(false);

    // Create a job queue
    JobQueue.Builder queueBuild =
        new JobQueue.Builder(queueName).setWorkflowConfig(workflowConfigBuilder.build());
    JobQueue queue = queueBuild.build();
    _driver.createQueue(queue);

    // Stop the queue to add jobs to the queue
    _driver.stop(queueName);

    // Keep track of the last jobName added
    String lastJobName = "";

    // First run some jobs with quotaType A
    List<TaskConfig> taskConfigs = new ArrayList<>();
    taskConfigs.add(new TaskConfig("ShortTask", new HashMap<String, String>()));
    JobConfig.Builder jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND)
        .addTaskConfigs(taskConfigs).setJobCommandConfigMap(_jobCommandMap).setQuotaType("A");

    for (int i = 0; i < 5; i++) {
      String jobName = "JOB_" + i;
      lastJobName = jobName;
      _driver.enqueueJob(queueName, jobName, jobConfigBulider);
    }

    // Resume the queue briefly and stop again to add more jobs
    _driver.resume(queueName);
    _driver.stop(queueName);

    // Run some jobs with quotaType B
    // First run some jobs with quotaType A
    taskConfigs = new ArrayList<>();
    taskConfigs.add(new TaskConfig("ShortTask", new HashMap<String, String>()));
    jobConfigBulider = new JobConfig.Builder().setCommand(JOB_COMMAND).addTaskConfigs(taskConfigs)
        .setJobCommandConfigMap(_jobCommandMap).setQuotaType("B");

    for (int i = 5; i < 10; i++) {
      String jobName = "JOB_" + i;
      lastJobName = jobName;
      _driver.enqueueJob(queueName, jobName, jobConfigBulider);
    }
    _driver.resume(queueName);
    _driver.pollForJobState(queueName, queueName + "_" + lastJobName, TaskState.COMPLETED);

    // Check run counts for each quota type
    Assert.assertEquals((int) _quotaTypeExecutionCount.get("A"), 5);
    Assert.assertEquals((int) _quotaTypeExecutionCount.get("B"), 5);
    Assert.assertFalse(_quotaTypeExecutionCount.containsKey(DEFAULT_QUOTA_TYPE));
  }

  /**
   * Helper method for creating custom workflows.
   * @param workflowName
   * @param shouldOverlapJobAssign
   * @param quotaType
   * @param numJobs
   * @param numTasks
   * @param taskType
   * @return a workflow per parameters given
   */
  private Workflow createWorkflow(String workflowName, boolean shouldOverlapJobAssign,
      String quotaType, int numJobs, int numTasks, String taskType) {

    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    WorkflowConfig.Builder configBuilder = new WorkflowConfig.Builder(workflowName);
    configBuilder.setAllowOverlapJobAssignment(shouldOverlapJobAssign);
    workflowBuilder.setWorkflowConfig(configBuilder.build());

    for (int jobIndex = 0; jobIndex < numJobs; jobIndex++) {
      String jobName = workflowName + "_" + jobIndex;
      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int taskIndex = 0; taskIndex < numTasks; taskIndex++) {
        Map<String, String> taskConfigMap = new HashMap<>();
        taskConfigs.add(new TaskConfig(taskType, taskConfigMap));
      }
      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setCommand(JOB_COMMAND).setJobCommandConfigMap(_jobCommandMap)
              .addTaskConfigs(taskConfigs).setQuotaType(quotaType);
      workflowBuilder.addJob(jobName, jobBuilder);
    }
    return workflowBuilder.build();
  }

  /**
   * A mock task class that models a short-lived task.
   */
  private class ShortTask extends MockTask {
    private final String _instanceName;
    private final String _quotaType;

    public ShortTask(TaskCallbackContext context, String instanceName) {
      super(context);
      _instanceName = instanceName;
      _quotaType = context.getJobConfig().getQuotaType();
      // Initialize the count for this quotaType if not already done
      if (_quotaType != null && !_quotaTypeExecutionCount.containsKey(_quotaType)) {
        _quotaTypeExecutionCount.put(_quotaType, 0);
      }
    }

    @Override
    public TaskResult run() {
      if (_quotaType != null) {
        _quotaTypeExecutionCount.put(_quotaType, _quotaTypeExecutionCount.get(_quotaType) + 1);
      }
      return new TaskResult(TaskResult.Status.COMPLETED,
          generateInfoMessageForDebugging(_instanceName, _quotaType));
    }
  }

  /**
   * A mock task class that models a long-running task.
   */
  private class LongTask extends MockTask {
    private final String _instanceName;
    private final String _quotaType;

    public LongTask(TaskCallbackContext context, String instanceName) {
      super(context);
      _instanceName = instanceName;
      _quotaType = context.getJobConfig().getQuotaType();
      // Initialize the count for this quotaType if not already done
      if (_quotaType != null && !_quotaTypeExecutionCount.containsKey(_quotaType)) {
        _quotaTypeExecutionCount.put(_quotaType, 0);
      }
    }

    @Override
    public TaskResult run() {
      if (_quotaType != null) {
        _quotaTypeExecutionCount.put(_quotaType, _quotaTypeExecutionCount.get(_quotaType) + 1);
      }
      try {
        Thread.sleep(LONG_RUNNING_TASK_DURATION);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return new TaskResult(TaskResult.Status.COMPLETED,
          generateInfoMessageForDebugging(_instanceName, _quotaType));
    }
  }

  /**
   * A mock task class that models a failed task.
   */
  private class FailTask extends MockTask {
    private final String _instanceName;
    private final String _quotaType;

    public FailTask(TaskCallbackContext context, String instanceName) {
      super(context);
      _instanceName = instanceName;
      _quotaType = context.getJobConfig().getQuotaType();
      // Initialize the count for this quotaType if not already done
      if (_quotaType != null && !_quotaTypeExecutionCount.containsKey(_quotaType)) {
        _quotaTypeExecutionCount.put(_quotaType, 0);
      }
    }

    @Override
    public TaskResult run() {
      if (_quotaType != null) {
        _quotaTypeExecutionCount.put(_quotaType, _quotaTypeExecutionCount.get(_quotaType) + 1);
      }
      return new TaskResult(TaskResult.Status.FAILED,
          generateInfoMessageForDebugging(_instanceName, _quotaType));
    }
  }

  /**
   * Helper method for generating info string for debugging purposes.
   * @param instanceName
   * @param quotaType
   * @return
   */
  private String generateInfoMessageForDebugging(String instanceName, String quotaType) {
    return String.format("Instance: %s, quotaType: %s", instanceName, quotaType);
  }
}