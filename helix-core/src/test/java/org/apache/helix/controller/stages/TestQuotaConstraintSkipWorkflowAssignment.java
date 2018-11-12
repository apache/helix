package org.apache.helix.controller.stages;

import java.util.Collections;
import java.util.HashMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.stages.task.TaskSchedulingStage;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.task.assigner.AssignableInstance;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestQuotaConstraintSkipWorkflowAssignment extends TaskTestBase {

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
    _controller.syncStop();
  }

  @Test
  public void testQuotaConstraintSkipWorkflowAssignment() throws Exception {
    ClusterEvent event = new ClusterEvent(ClusterEventType.Unknown);
    ClusterDataCache cache = new ClusterDataCache(CLUSTER_NAME);
    JobConfig.Builder job = new JobConfig.Builder();

    job.setJobCommandConfigMap(Collections.singletonMap(MockTask.JOB_DELAY, "100000"));
    TaskDriver driver = new TaskDriver(_manager);
    for (int i = 0; i < 10; i++) {
      Workflow.Builder workflow = new Workflow.Builder("Workflow" + i);
      job.setWorkflow("Workflow" + i);
      TaskConfig taskConfig =
          new TaskConfig(MockTask.TASK_COMMAND, new HashMap<String, String>(), null, null);
      job.addTaskConfigMap(Collections.singletonMap(taskConfig.getId(), taskConfig));
      job.setJobId(TaskUtil.getNamespacedJobName("Workflow" + i, "JOB"));
      workflow.addJob("JOB", job);
      driver.start(workflow.build());
    }
    ConfigAccessor accessor = new ConfigAccessor(_gZkClient);
    ClusterConfig clusterConfig = accessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setTaskQuotaRatio(AssignableInstance.DEFAULT_QUOTA_TYPE, 3);
    clusterConfig.setTaskQuotaRatio("OtherType", 37);
    accessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    cache.setTaskCache(true);
    cache.refresh(_manager.getHelixDataAccessor());
    event.addAttribute(AttributeName.ClusterDataCache.name(), cache);
    event.addAttribute(AttributeName.helixmanager.name(), _manager);
    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new TaskSchedulingStage());
    Assert.assertTrue(!cache.getAssignableInstanceManager()
        .hasGlobalCapacity(AssignableInstance.DEFAULT_QUOTA_TYPE));
    BestPossibleStateOutput bestPossibleStateOutput =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    Assert.assertTrue(bestPossibleStateOutput.getStateMap().size() == 3);
  }
}
