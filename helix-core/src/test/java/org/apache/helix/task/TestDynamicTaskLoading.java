package org.apache.helix.task;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.examples.MasterSlaveStateModelFactory;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for dynamically-loaded tasks. Different scenarios
 * for success and failure are tested. Failure scenarios include
 * non-existing JAR file, non-existing DynamicTaskConfig in
 * ZooKeeper, non-existing Task class, and non-existing
 * TaskFactory class.
 */
public class TestDynamicTaskLoading extends ZkTestBase {
  private static final String CLUSTER_NAME = CLUSTER_PREFIX + "_TestDynamicTaskLoading";
  private static final String INSTANCE_NAME = "localhost_12913";
  protected final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private HelixManager _manager;
  private MockParticipantManager _participant;
  private ClusterControllerManager _controller;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    // BeforeMethod is required (as opposed to BeforeClass)
    // to make sure each test starts with a fresh state.
    // This is important because we are testing dynamically
    // loading classes, and the tests would break if the
    // classes are already loaded.
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, INSTANCE_NAME);
    _participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, INSTANCE_NAME);
    StateModelFactory<StateModel> stateModelFactory =
        new MasterSlaveStateModelFactory(INSTANCE_NAME);
    StateMachineEngine stateMach = _participant.getStateMachineEngine();
    stateMach.registerStateModelFactory(MASTER_SLAVE_STATE_MODEL, stateModelFactory);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    _participant.getStateMachineEngine().registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
        new TaskStateModelFactory(_participant, taskFactoryReg));
    _participant.syncStart();

    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, null);
    _controller.syncStart();
  }

  private void removePathIfExists(String path) {
    if (_manager.getHelixDataAccessor().getBaseDataAccessor().exists(path, 0)) {
      _manager.getHelixDataAccessor().getBaseDataAccessor().remove(path, 0);
    }
  }

  /**
   * Submit a workflow consisting of a job with a MockTask task.
   * @param workflowName name of the workflow
   * @param driver {@link TaskDriver} to submit workflowName to
   */
  private void submitWorkflow(String workflowName, TaskDriver driver) {
    JobConfig.Builder job = new JobConfig.Builder();
    job.setJobCommandConfigMap(Collections.singletonMap(MockTask.JOB_DELAY, "100"));
    Workflow.Builder workflow = new Workflow.Builder(workflowName);
    job.setWorkflow(workflowName);
    TaskConfig taskConfig =
        new TaskConfig(MockTask.TASK_COMMAND, new HashMap<String, String>(), null, null);
    job.addTaskConfigMap(Collections.singletonMap(taskConfig.getId(), taskConfig));
    job.setJobId(TaskUtil.getNamespacedJobName(workflowName, "JOB"));
    workflow.addJob("JOB", job);
    driver.start(workflow.build());
  }

  @Test
  public void testDynamicTaskLoading() throws Exception {
    String taskCommand = "Reindex";
    String taskJarPath = "src/test/resources/Reindex.jar";
    String taskVersion = "1.0.0";
    String fullyQualifiedTaskClassName = "com.mycompany.mocktask.MockTask";
    String fullyQualifiedTaskFactoryClassName = "com.mycompany.mocktask.MockTaskFactory";

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add(fullyQualifiedTaskClassName);
    DynamicTaskConfig taskConfig =
        new DynamicTaskConfig(taskCommand, taskJarPath, taskVersion, taskClasses,
            fullyQualifiedTaskFactoryClassName);
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    // Wait for the workflow to either complete or fail.
    TaskState finalState =
        driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(finalState, TaskState.COMPLETED);
  }

  @Test
  public void testDynamicTaskLoadingNonexistingJar() throws Exception {
    String taskCommand = "Reindex";
    String taskJarPath = "src/test/resources/Random.jar";
    String taskVersion = "1.0.0";
    String fullyQualifiedTaskClassName = "com.mycompany.mocktask.MockTask";
    String fullyQualifiedTaskFactoryClassName = "com.mycompany.mocktask.MockTaskFactory";

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add(fullyQualifiedTaskClassName);
    DynamicTaskConfig taskConfig =
        new DynamicTaskConfig(taskCommand, taskJarPath, taskVersion, taskClasses,
            fullyQualifiedTaskFactoryClassName);
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    // Wait for the workflow to either complete or fail.
    TaskState finalState =
        driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(finalState, TaskState.FAILED);
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskConfig() throws Exception {
    String taskCommand = "Reindex";

    // Remove task config ZNRecord if it exists.
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    removePathIfExists(path);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    // Wait for the workflow to either complete or fail.
    TaskState finalState =
        driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(finalState, TaskState.FAILED);
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskClass() throws Exception {
    String taskCommand = "Reindex";
    String taskJarPath = "src/test/resources/Random.jar";
    String taskVersion = "1.0.0";
    String fullyQualifiedTaskClassName = "com.mycompany.mocktask.RandomTask";
    String fullyQualifiedTaskFactoryClassName = "com.mycompany.mocktask.MockTaskFactory";

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add(fullyQualifiedTaskClassName);
    DynamicTaskConfig taskConfig =
        new DynamicTaskConfig(taskCommand, taskJarPath, taskVersion, taskClasses,
            fullyQualifiedTaskFactoryClassName);
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    // Wait for the workflow to either complete or fail.
    TaskState finalState =
        driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(finalState, TaskState.FAILED);
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskFactory() throws Exception {
    String taskCommand = "Reindex";
    String taskJarPath = "src/test/resources/Random.jar";
    String taskVersion = "1.0.0";
    String fullyQualifiedTaskClassName = "com.mycompany.mocktask.MockTask";
    String fullyQualifiedTaskFactoryClassName = "com.mycompany.mocktask.RandomTaskFactory";

    // Add task definition information as a DynamicTaskConfig.
    List<String> taskClasses = new ArrayList();
    taskClasses.add(fullyQualifiedTaskClassName);
    DynamicTaskConfig taskConfig =
        new DynamicTaskConfig(taskCommand, taskJarPath, taskVersion, taskClasses,
            fullyQualifiedTaskFactoryClassName);
    String path = TaskConstants.DYNAMICALLY_LOADED_TASK_PATH + "/" + taskCommand;
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, taskConfig.getTaskConfigZNRecord(), AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    // Wait for the workflow to either complete or fail.
    TaskState finalState =
        driver.pollForWorkflowState(workflowName, TaskState.COMPLETED, TaskState.FAILED);
    Assert.assertEquals(finalState, TaskState.FAILED);
  }

  @AfterMethod
  public void afterMethod() {
    // AfterMethod is required (as opposed to AfterClass)
    // to make sure each test erases all of its state.
    // This is important because we are testing dynamically
    // loading classes, and the tests would break if the
    // classes are already loaded.
    _controller.syncStop();
    _manager.disconnect();
    _participant.syncStop();

    // Shutdown the state model factories to close all threads.
    StateMachineEngine stateMachine = _participant.getStateMachineEngine();
    if (stateMachine != null) {
      StateModelFactory stateModelFactory =
          stateMachine.getStateModelFactory(TaskConstants.STATE_MODEL_NAME);
      if (stateModelFactory != null && stateModelFactory instanceof TaskStateModelFactory) {
        ((TaskStateModelFactory) stateModelFactory).shutdownNow();
      }
    }
    deleteCluster(CLUSTER_NAME);
  }
}
