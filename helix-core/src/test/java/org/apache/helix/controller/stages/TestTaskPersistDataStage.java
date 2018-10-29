package org.apache.helix.controller.stages;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.stages.task.TaskPersistDataStage;
import org.apache.helix.participant.MockZKHelixManager;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskPersistDataStage extends ZkTestBase {
  private String CLUSTER_NAME = "TestCluster_" + TestHelper.getTestClassName();
  private HelixManager _helixManager;
  private TaskDriver _driver;

  @BeforeClass
  public void beforeClass() {
    _helixManager = new MockZKHelixManager(CLUSTER_NAME, "MockInstance", InstanceType.ADMINISTRATOR,
        _gZkClient);
    _driver = new TaskDriver(_gZkClient, CLUSTER_NAME);
  }

  @Test
  public void testTaskContextUpdate() {
    ClusterEvent event = new ClusterEvent(CLUSTER_NAME, ClusterEventType.CurrentStateChange);
    event.addAttribute(AttributeName.helixmanager.name(), _helixManager);
    TaskPersistDataStage persistDataStage = new TaskPersistDataStage();

    ClusterDataCache cache = new ClusterDataCache(CLUSTER_NAME);
    TaskDataCache taskDataCache = cache.getTaskDataCache();
    String testWorkflow = TestHelper.getTestMethodName();
    String testJobPrefix = testWorkflow + "_Job_";

    WorkflowContext wfCtx = new WorkflowContext(new ZNRecord(testWorkflow));
    wfCtx.setJobState(testJobPrefix + "0", TaskState.IN_PROGRESS);
    wfCtx.setJobState(testJobPrefix + "1", TaskState.COMPLETED);
    wfCtx.setWorkflowState(TaskState.IN_PROGRESS);
    wfCtx.setName(testWorkflow);
    wfCtx.setStartTime(System.currentTimeMillis());

    JobContext jbCtx0 = new JobContext(new ZNRecord(testJobPrefix + "0"));
    jbCtx0.setName(testJobPrefix + "0");
    jbCtx0.setStartTime(System.currentTimeMillis());
    jbCtx0.setPartitionState(0, TaskPartitionState.RUNNING);

    JobContext jbCtx1 = new JobContext((new ZNRecord(testJobPrefix + "1")));
    jbCtx1.setName(testJobPrefix + "1");
    jbCtx1.setStartTime(System.currentTimeMillis());
    jbCtx1.setPartitionState(0, TaskPartitionState.COMPLETED);

    taskDataCache.updateWorkflowContext(testWorkflow, wfCtx);
    taskDataCache.updateJobContext(testJobPrefix + "0", jbCtx0);
    taskDataCache.updateJobContext(testJobPrefix + "1", jbCtx1);

    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);
    persistDataStage.process(event);

    jbCtx0.setPartitionState(0, TaskPartitionState.ERROR);
    wfCtx.setJobState(testJobPrefix + "0", TaskState.FAILED);
    taskDataCache.updateJobContext(testJobPrefix + "0", jbCtx0);

    wfCtx.getJobStates().remove(testJobPrefix + "1");
    taskDataCache.removeContext(testJobPrefix + "1");

    JobContext jbCtx2 = new JobContext(new ZNRecord(testJobPrefix + "2"));
    jbCtx2.setName(testJobPrefix + "2");
    jbCtx2.setPartitionState(1, TaskPartitionState.INIT);
    wfCtx.setJobState(testJobPrefix + "2", TaskState.IN_PROGRESS);
    taskDataCache.updateJobContext(testJobPrefix + "2", jbCtx2);

    taskDataCache.updateWorkflowContext(testWorkflow, wfCtx);
    persistDataStage.process(event);

    Assert.assertEquals(_driver.getWorkflowContext(testWorkflow), wfCtx);
    Assert.assertEquals(_driver.getJobContext(testJobPrefix + "0"), jbCtx0);
    Assert.assertEquals(_driver.getJobContext(testJobPrefix + "2"), jbCtx2);
    Assert.assertNull(_driver.getJobContext(testJobPrefix + "1"));

  }
}
