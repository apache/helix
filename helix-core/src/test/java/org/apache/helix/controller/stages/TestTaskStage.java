package org.apache.helix.controller.stages;

import org.apache.helix.AccessOption;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.stages.task.TaskPersistDataStage;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskStage extends TaskTestBase {
  private ClusterEvent _event = new ClusterEvent(CLUSTER_NAME, ClusterEventType.CurrentStateChange);
  private PropertyKey.Builder _keyBuilder;
  private String _testWorkflow = TestHelper.getTestClassName();
  private String _testJobPrefix = _testWorkflow + "_Job_";

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    // Stop the controller for isolated testing of the stage
    _controller.syncStop();
    _keyBuilder = _manager.getHelixDataAccessor().keyBuilder();
  }

  @Test
  public void testPersistContextData() {
    _event.addAttribute(AttributeName.helixmanager.name(), _manager);

    ClusterDataCache cache = new ClusterDataCache(CLUSTER_NAME);
    cache.setTaskCache(true);
    TaskDataCache taskDataCache = cache.getTaskDataCache();

    // Build a queue
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(_testWorkflow);
    JobConfig.Builder jobBuilder_0 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    JobConfig.Builder jobBuilder_1 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    JobConfig.Builder jobBuilder_2 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    queueBuilder.enqueueJob("Job_0", jobBuilder_0).enqueueJob("Job_1", jobBuilder_1)
        .enqueueJob("Job_2", jobBuilder_2);

    _driver.createQueue(queueBuilder.build());
    // Manually trigger a cache refresh
    cache.refresh(new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor));

    // Create the IdealState ZNode for the jobs
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, _testJobPrefix + "0", 1,
        TaskConstants.STATE_MODEL_NAME);
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, _testJobPrefix + "1", 1,
        TaskConstants.STATE_MODEL_NAME);
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, _testJobPrefix + "2", 1,
        TaskConstants.STATE_MODEL_NAME);

    // Create the context
    WorkflowContext wfCtx = new WorkflowContext(new ZNRecord(_testWorkflow));
    wfCtx.setJobState(_testJobPrefix + "0", TaskState.COMPLETED);
    wfCtx.setJobState(_testJobPrefix + "1", TaskState.COMPLETED);
    wfCtx.setWorkflowState(TaskState.IN_PROGRESS);
    wfCtx.setName(_testWorkflow);
    wfCtx.setStartTime(System.currentTimeMillis());

    JobContext jbCtx0 = new JobContext(new ZNRecord(_testJobPrefix + "0"));
    jbCtx0.setName(_testJobPrefix + "0");
    jbCtx0.setStartTime(System.currentTimeMillis());
    jbCtx0.setPartitionState(0, TaskPartitionState.COMPLETED);

    JobContext jbCtx1 = new JobContext((new ZNRecord(_testJobPrefix + "1")));
    jbCtx1.setName(_testJobPrefix + "1");
    jbCtx1.setStartTime(System.currentTimeMillis());
    jbCtx1.setPartitionState(0, TaskPartitionState.COMPLETED);

    taskDataCache.updateWorkflowContext(_testWorkflow, wfCtx);
    taskDataCache.updateJobContext(_testJobPrefix + "0", jbCtx0);
    taskDataCache.updateJobContext(_testJobPrefix + "1", jbCtx1);

    _event.addAttribute(AttributeName.ClusterDataCache.name(), cache);

    // Write contexts to ZK first
    TaskPersistDataStage persistDataStage = new TaskPersistDataStage();
    persistDataStage.process(_event);

    Assert.assertNotNull(_driver.getWorkflowContext(_testWorkflow));
    Assert.assertNotNull(_driver.getJobContext(_testJobPrefix + "0"));
    Assert.assertNotNull(_driver.getJobContext(_testJobPrefix + "1"));

    jbCtx0.setPartitionState(0, TaskPartitionState.ERROR);
    wfCtx.setJobState(_testJobPrefix + "0", TaskState.FAILED);
    taskDataCache.updateJobContext(_testJobPrefix + "0", jbCtx0);

    wfCtx.getJobStates().remove(_testJobPrefix + "1");
    taskDataCache.removeContext(_testJobPrefix + "1");

    JobContext jbCtx2 = new JobContext(new ZNRecord(_testJobPrefix + "2"));
    jbCtx2.setName(_testJobPrefix + "2");
    jbCtx2.setPartitionState(1, TaskPartitionState.INIT);
    wfCtx.setJobState(_testJobPrefix + "2", TaskState.IN_PROGRESS);
    taskDataCache.updateJobContext(_testJobPrefix + "2", jbCtx2);

    taskDataCache.updateWorkflowContext(_testWorkflow, wfCtx);

    persistDataStage.process(_event);

    Assert.assertEquals(_driver.getWorkflowContext(_testWorkflow), wfCtx);
    Assert.assertEquals(_driver.getJobContext(_testJobPrefix + "0"), jbCtx0);
    Assert.assertEquals(_driver.getJobContext(_testJobPrefix + "2"), jbCtx2);
    Assert.assertNull(_driver.getJobContext(_testJobPrefix + "1"));
  }

  /**
   * Test that if there is a job in the DAG with JobConfig gone (due to ZK delete failure), the
   * async job purge will try to delete it again.
   */
  @Test(dependsOnMethods = "testPersistContextData")
  public void testPartialDataPurge() {
    // Manually delete JobConfig
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "0");
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "1");
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "2");

    // Then purge jobs
    TaskGarbageCollectionStage garbageCollectionStage = new TaskGarbageCollectionStage();
    garbageCollectionStage.execute(_event);

    // Check that IS and contexts have been purged for the 2 jobs in both old and new ZNode paths
    // IdealState check
    checkForIdealStateAndContextRemoval(_testWorkflow, _testJobPrefix + "0");
    checkForIdealStateAndContextRemoval(_testWorkflow, _testJobPrefix + "1");
    checkForIdealStateAndContextRemoval(_testWorkflow, _testJobPrefix + "2");
  }

  private void deleteJobConfigs(String workflowName, String jobName) {
    String oldPath = _manager.getHelixDataAccessor().keyBuilder().resourceConfig(jobName).getPath();
    String newPath = _manager.getHelixDataAccessor().keyBuilder()
        .jobConfigZNode(workflowName, jobName).getPath();
    _baseAccessor.remove(oldPath, AccessOption.PERSISTENT);
    _baseAccessor.remove(newPath, AccessOption.PERSISTENT);
  }

  private void checkForIdealStateAndContextRemoval(String workflow, String job) {
    // IdealState
    Assert.assertFalse(
        _baseAccessor.exists(_keyBuilder.idealStates(job).getPath(), AccessOption.PERSISTENT));

    // JobContexts in old ZNode path
    String oldPath =
        String.format("/%s/PROPERTYSTORE/TaskRebalancer/%s/Context", CLUSTER_NAME, job);
    String newPath = _keyBuilder.jobContextZNode(workflow, job).getPath();
    Assert.assertFalse(_baseAccessor.exists(oldPath, AccessOption.PERSISTENT));
    Assert.assertFalse(_baseAccessor.exists(newPath, AccessOption.PERSISTENT));
  }
}
