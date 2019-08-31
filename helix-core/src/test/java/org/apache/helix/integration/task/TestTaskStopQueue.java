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

import com.google.common.collect.ImmutableMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This test makes sure the workflow can be stopped if previousAssignment and currentState are
 * deleted.
 */
public class TestTaskStopQueue extends TaskTestBase {
  private static final long TIMEOUT = 200000L;
  private static final String EXECUTION_TIME = "100000";
  private HelixAdmin _admin;
  protected HelixManager _manager;
  protected TaskDriver _driver;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _manager = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();
    _driver = new TaskDriver(_manager);
    _admin = _gSetupTool.getClusterManagementTool();
  }

  @Test
  public void testStopRunningQueue() throws InterruptedException {
    String jobQueueName = TestHelper.getTestMethodName();

    JobConfig.Builder jobBuilder0 = JobConfig.Builder.fromMap(WorkflowGenerator.DEFAULT_JOB_CONFIG)
        .setTimeoutPerTask(TIMEOUT).setMaxAttemptsPerTask(10).setWorkflow(jobQueueName)
        .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, EXECUTION_TIME));

    JobQueue.Builder jobQueue = TaskTestUtil.buildJobQueue(jobQueueName);
    jobQueue.enqueueJob("JOB0", jobBuilder0);

    _driver.start(jobQueue.build());

    _driver.pollForWorkflowState(jobQueueName, TaskState.IN_PROGRESS);
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.IN_PROGRESS);

    _controller.syncStop();

    _driver.stop(jobQueueName);

    String namespacedJobName = TaskUtil.getNamespacedJobName(jobQueueName, "JOB0");

    for (int i = 0; i < _numNodes; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (_startPort + i);
      ZkClient client = (ZkClient) _participants[i].getZkClient();
      String sessionId = ZkTestHelper.getSessionId(client);
      String currentStatePath = "/" + CLUSTER_NAME + "/INSTANCES/" + instance + "/CURRENTSTATES/"
          + sessionId + "/" + namespacedJobName;
      _manager.getHelixDataAccessor().getBaseDataAccessor().remove(currentStatePath,
          AccessOption.PERSISTENT);
      Assert.assertFalse(_manager.getHelixDataAccessor().getBaseDataAccessor()
          .exists(currentStatePath, AccessOption.PERSISTENT));
    }

    String previousAssignment = "/" + CLUSTER_NAME + "/PROPERTYSTORE/TaskRebalancer/"
        + namespacedJobName + "/PreviousResourceAssignment";
    _manager.getHelixDataAccessor().getBaseDataAccessor().remove(previousAssignment,
        AccessOption.PERSISTENT);
    Assert.assertFalse(_manager.getHelixDataAccessor().getBaseDataAccessor()
        .exists(previousAssignment, AccessOption.PERSISTENT));

    // Start the Controller
    String controllerName = CONTROLLER_PREFIX + "_1";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _driver.pollForWorkflowState(jobQueueName, TaskState.STOPPED);
    _driver.pollForJobState(jobQueueName, TaskUtil.getNamespacedJobName(jobQueueName, "JOB0"),
        TaskState.STOPPED);
  }
}
