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
import org.apache.helix.HelixException;
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
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestDynamicTaskLoading extends ZkTestBase {
  private final String _clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  private static final String _instanceName = "localhost_12913";
  private HelixManager _manager;
  private MockParticipantManager _participant;
  private ClusterControllerManager _controller;

  @BeforeMethod
  public void beforeMethod() throws Exception {
    //super.beforeClass();
    _gSetupTool.addCluster(_clusterName, true);

    _manager = HelixManagerFactory
        .getZKHelixManager(_clusterName, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _gSetupTool.addInstanceToCluster(_clusterName, _instanceName);
    _participant = new MockParticipantManager(ZK_ADDR, _clusterName, _instanceName);
    StateModelFactory<StateModel> stateModelFactory =
        new MasterSlaveStateModelFactory(_instanceName, 0);
    StateMachineEngine stateMach = _participant.getStateMachineEngine();
    stateMach.registerStateModelFactory("MasterSlave", stateModelFactory);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    _participant.getStateMachineEngine().registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
        new TaskStateModelFactory(_participant, taskFactoryReg));
    _participant.syncStart();

    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName, null);
    _controller.syncStart();
  }

  private ZNRecord createTaskConfig(String id, String jar, String version, List<String> taskClasses,
      String taskFactory) {
    ZNRecord configZnRecord = new ZNRecord(id);
    configZnRecord.setSimpleField(TaskConstants.TASK_JAR_FILE_KEY, jar);
    configZnRecord.setSimpleField(TaskConstants.TASK_VERSION_KEY, version);
    configZnRecord.setListField(TaskConstants.TASK_CLASSES_KEY, taskClasses);
    configZnRecord.setSimpleField(TaskConstants.TASK_FACTORY_KEY, taskFactory);
    return configZnRecord;
  }

  private void removePathIfExists(String path) {
    if (_manager.getHelixDataAccessor().getBaseDataAccessor().exists(path, 0)) {
      _manager.getHelixDataAccessor().getBaseDataAccessor().remove(path, 0);
    }
  }

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
    // Add task definition information as a ZNRecord.
    List<String> taskClasses = new ArrayList<String>();
    taskClasses.add("com.mycompany.mocktask.MockTask");
    ZNRecord configZnRecord =
        createTaskConfig("Reindex", "src/test/resources/Reindex.jar", "1.0.0", taskClasses,
            "com.mycompany.mocktask.MockTaskFactory");
    String path = TaskConstants.TASK_PATH + "/Reindex";
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, configZnRecord, AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    try {
      TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.COMPLETED);
      AssertJUnit.assertEquals(finalState, TaskState.COMPLETED);
    } catch (HelixException e) {
      AssertJUnit.fail(e.getMessage());
    }
  }

  @Test
  public void testDynamicTaskLoadingNonexistingJar() throws Exception {
    // Add task definition information as a ZNRecord.
    List<String> taskClasses = new ArrayList<String>();
    taskClasses.add("com.mycompany.mocktask.MockTask");
    ZNRecord configZnRecord =
        createTaskConfig("Reindex", "src/test/resources/Random.jar", "1.0.0", taskClasses,
            "com.mycompany.mocktask.MockTaskFactory");
    String path = TaskConstants.TASK_PATH + "/Reindex";
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, configZnRecord, AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    try {
      TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.FAILED);
      AssertJUnit.assertEquals(finalState, TaskState.FAILED);
    } catch (HelixException e) {
      AssertJUnit.fail(e.getMessage());
    }
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskConfig() throws Exception {
    // Remove task config ZNRecord if it exists.
    String path = TaskConstants.TASK_PATH + "/Reindex";
    removePathIfExists(path);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    try {
      TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.FAILED);
      AssertJUnit.assertEquals(finalState, TaskState.FAILED);
    } catch (HelixException e) {
      AssertJUnit.fail(e.getMessage());
    }
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskClass() throws Exception {
    // Add task definition information as a ZNRecord.
    List<String> taskClasses = new ArrayList<String>();
    taskClasses.add("com.mycompany.mocktask.RandomTask");
    ZNRecord configZnRecord =
        createTaskConfig("Reindex", "src/test/resources/Reindex.jar", "1.0.0", taskClasses,
            "com.mycompany.mocktask.MockTaskFactory");
    String path = TaskConstants.TASK_PATH + "/Reindex";
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, configZnRecord, AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    try {
      TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.FAILED);
      AssertJUnit.assertEquals(finalState, TaskState.FAILED);
    } catch (HelixException e) {
      AssertJUnit.fail(e.getMessage());
    }
  }

  @Test
  public void testDynamicTaskLoadingNonexistingTaskFactory() throws Exception {
    // Add task definition information as a ZNRecord.
    List<String> taskClasses = new ArrayList<String>();
    taskClasses.add("com.mycompany.mocktask.MockTask");
    ZNRecord configZnRecord =
        createTaskConfig("Reindex", "src/test/resources/Reindex.jar", "1.0.0", taskClasses,
            "com.mycompany.mocktask.RandomTaskFactory");
    String path = TaskConstants.TASK_PATH + "/Reindex";
    removePathIfExists(path);
    _manager.getHelixDataAccessor().getBaseDataAccessor()
        .create(path, configZnRecord, AccessOption.PERSISTENT);

    // Submit workflow
    TaskDriver driver = new TaskDriver(_manager);
    String workflowName = TestHelper.getTestMethodName();
    submitWorkflow(workflowName, driver);

    try {
      TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.FAILED);
      AssertJUnit.assertEquals(finalState, TaskState.FAILED);
    } catch (HelixException e) {
      AssertJUnit.fail(e.getMessage());
    }
  }

  @AfterMethod
  public void afterMethod() {
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
    deleteCluster(_clusterName);
  }
}
