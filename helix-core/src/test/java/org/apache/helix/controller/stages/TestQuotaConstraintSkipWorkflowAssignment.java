package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.HashMap;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
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
    WorkflowControllerDataProvider cache = new WorkflowControllerDataProvider(CLUSTER_NAME);
    TaskDriver driver = new TaskDriver(_manager);
    for (int i = 0; i < 10; i++) {
      Workflow.Builder workflow = new Workflow.Builder("Workflow" + i);
      JobConfig.Builder job = new JobConfig.Builder();
      job.setJobCommandConfigMap(Collections.singletonMap(MockTask.JOB_DELAY, "100000"));
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
    cache.refresh(_manager.getHelixDataAccessor());
    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
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
