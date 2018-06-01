package org.apache.helix.integration.task;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.TestHelper;
import org.apache.helix.model.IdealState;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFailTargetJobWhenResourceDisabled extends TaskTestBase {
  private JobConfig.Builder _jobCfg;
  private String _jobName;

  @BeforeClass
  public void beforeClass() throws Exception {
    setSingleTestEnvironment();
    super.beforeClass();
    _jobName = "TestJob";
    _jobCfg = new JobConfig.Builder().setJobId(_jobName).setCommand(MockTask.TASK_COMMAND)
        .setTargetResource(WorkflowGenerator.DEFAULT_TGT_DB);
  }

  @Test
  public void testJobScheduleAfterResourceDisabled() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    _gSetupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    workflow.addJob(_jobName, _jobCfg);
    _driver.start(workflow.build());

    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
  }

  @Test
  public void testJobScheduleBeforeResourceDisabled() throws InterruptedException {
    String workflowName = TestHelper.getTestMethodName();
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    _jobCfg.setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000000"));
    workflow.addJob(_jobName, _jobCfg);
    _driver.start(workflow.build());
    Thread.sleep(1000);
    _gSetupTool.getClusterManagementTool()
        .enableResource(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, false);
    _driver.pollForWorkflowState(workflowName, TaskState.FAILED);
  }


}
