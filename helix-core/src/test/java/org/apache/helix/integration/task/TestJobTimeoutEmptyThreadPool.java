package org.apache.helix.integration.task;

import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModel;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowConfig;
import org.apache.helix.tools.ClusterSetup;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestJobTimeoutEmptyThreadPool extends TaskSynchronizedTestBase {

  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numNodes = 1;
    _numParitions = 50;
    _numReplicas = 1;

    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipantsWithStuckTaskStateModelFactory();
    createManagers();
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, CONTROLLER_PREFIX);
    _controller.syncStart();

    // Enable cancellation
    ConfigAccessor _configAccessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.stateTransitionCancelEnabled(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  protected void startParticipantsWithStuckTaskStateModelFactory() {
    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, new TaskFactory() {
      @Override public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    // start dummy participants
    for (int i = 0; i < _numNodes; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task",
          new StuckTaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }
  }

  @Test
  public void testEmptyThreadPool() throws InterruptedException {
    // wait till basic test DB setup finish
    Thread.sleep(1000);

    final String BLOCK_WORKFLOW_NAME = "blockWorkflow";
    final String TIMEOUT_WORKFLOW_NAME = "timeoutWorkflow";
    final String DB_NAME = WorkflowGenerator.DEFAULT_TGT_DB;
    final String TIMEOUT_JOB_1 = "timeoutJob1";
    final String TIMEOUT_JOB_2 = "timeoutJob2";

    // 50 block jobs
    JobConfig.Builder blockJobBuilder = new JobConfig.Builder()
        .setWorkflow(BLOCK_WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setNumConcurrentTasksPerInstance(50);

    Workflow.Builder blockWorkflowBuilder = new Workflow.Builder(BLOCK_WORKFLOW_NAME)
        .addJob("blockJob", blockJobBuilder);
    _driver.start(blockWorkflowBuilder.build());

    Thread.sleep(1000);

    // Now, the Helix task threadpool is full and blocked by blockJob.
    // New tasks assigned to the instance won't start at all.

    // 2 timeout jobs, first one timeout, but won't block the second one to run, the second one also timeout.
    JobConfig.Builder timeoutJobBuilder = new JobConfig.Builder()
        .setWorkflow(TIMEOUT_WORKFLOW_NAME)
        .setTargetResource(DB_NAME)
        .setTargetPartitionStates(Sets.newHashSet(MasterSlaveSMD.States.MASTER.name()))
        .setCommand(MockTask.TASK_COMMAND)
        .setNumConcurrentTasksPerInstance(50)
        .setTimeout(3000); // Wait a bit so that tasks are already assigned to the job (and will be cancelled)

    WorkflowConfig.Builder timeoutWorkflowConfigBuilder = new WorkflowConfig.Builder()
        .setFailureThreshold(1); // workflow ignores first job's timeout and schedule second job and succeed.

    Workflow.Builder timeoutWorkflowBuilder = new Workflow.Builder(TIMEOUT_WORKFLOW_NAME)
        .setWorkflowConfig(timeoutWorkflowConfigBuilder.build())
        .addJob(TIMEOUT_JOB_1, timeoutJobBuilder); // job 1 timeout, but won't block job 2

    timeoutJobBuilder.setIgnoreDependentJobFailure(true); // ignore first job's timeout

    timeoutWorkflowBuilder.addJob(TIMEOUT_JOB_2, timeoutJobBuilder) // job 2 also timeout
        .addParentChildDependency(TIMEOUT_JOB_1, TIMEOUT_JOB_2);

    _driver.start(timeoutWorkflowBuilder.build());

    _driver.pollForJobState(TIMEOUT_WORKFLOW_NAME, TaskUtil.getNamespacedJobName(TIMEOUT_WORKFLOW_NAME, TIMEOUT_JOB_1),
        TaskState.TIMED_OUT);
    _driver.pollForJobState(TIMEOUT_WORKFLOW_NAME, TaskUtil.getNamespacedJobName(TIMEOUT_WORKFLOW_NAME, TIMEOUT_JOB_2),
        TaskState.TIMED_OUT);
    _driver.pollForWorkflowState(TIMEOUT_WORKFLOW_NAME, TaskState.FAILED);

    JobContext jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(TIMEOUT_WORKFLOW_NAME, TIMEOUT_JOB_1));
    for (int pId : jobContext.getPartitionSet()) {
      // All tasks stuck at INIT->RUNNING, and state transition cancelled and marked TASK_ABORTED
      Assert.assertEquals(jobContext.getPartitionState(pId), TaskPartitionState.TASK_ABORTED);
    }

    jobContext = _driver.getJobContext(TaskUtil.getNamespacedJobName(TIMEOUT_WORKFLOW_NAME, TIMEOUT_JOB_2));
    for (int pId : jobContext.getPartitionSet()) {
      // All tasks stuck at INIT->RUNNING, and state transition cancelled and marked TASK_ABORTED
      Assert.assertEquals(jobContext.getPartitionState(pId), TaskPartitionState.TASK_ABORTED);
    }
  }

  public static class StuckTaskStateModel extends TaskStateModel {
    private StuckTaskStateModel(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry,
        ScheduledExecutorService taskExecutor) {
      super(manager, taskFactoryRegistry, taskExecutor);
    }

    @Override
    public void onBecomeRunningFromInit(Message msg, NotificationContext context) {
      try {
        Thread.sleep(99999999);
      } catch (InterruptedException e) {

      }
      super.onBecomeRunningFromInit(msg, context);
    }
  }

  private static class StuckTaskStateModelFactory extends StateModelFactory<StuckTaskStateModel> {
    private final HelixManager _manager;
    private final Map<String, TaskFactory> _taskFactoryRegistry;
    private final ScheduledExecutorService _taskExecutor;
    private final static int TASK_THREADPOOL_SIZE = 40;

    public StuckTaskStateModelFactory(HelixManager manager, Map<String, TaskFactory> taskFactoryRegistry) {
      _manager = manager;
      _taskFactoryRegistry = taskFactoryRegistry;
      _taskExecutor = Executors.newScheduledThreadPool(TASK_THREADPOOL_SIZE, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "TaskStateModel-thread-pool");
          }
        });
    }

    @Override public StuckTaskStateModel createNewStateModel(String resourceName, String partitionKey) {
      return new StuckTaskStateModel(_manager, _taskFactoryRegistry, _taskExecutor);
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
}
