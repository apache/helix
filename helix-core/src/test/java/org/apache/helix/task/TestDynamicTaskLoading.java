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
import org.apache.zookeeper.CreateMode;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestDynamicTaskLoading extends ZkTestBase {
  private final String _clusterName = CLUSTER_PREFIX + "_" + getShortClassName();
  private static final String _instanceName = "localhost_12913";
  private HelixManager _manager;
  private MockParticipantManager _participant;
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    _gSetupTool.addCluster(_clusterName, true);

    _manager = HelixManagerFactory.getZKHelixManager(_clusterName, "Admin",
        InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    _gSetupTool.addInstanceToCluster(_clusterName, _instanceName);
    _participant = new MockParticipantManager(ZK_ADDR, _clusterName, _instanceName);
    StateModelFactory<StateModel> stateModelFactory = new MasterSlaveStateModelFactory(_instanceName, 0);
    StateMachineEngine stateMach = _participant.getStateMachineEngine();
    stateMach.registerStateModelFactory("MasterSlave", stateModelFactory);
    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    _participant.getStateMachineEngine().registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
        new TaskStateModelFactory(_participant, taskFactoryReg));
    _participant.syncStart();

    _controller = new ClusterControllerManager(ZK_ADDR, _clusterName,null);
    _controller.syncStart();

    // Add task definition information as a ZNRecord.
    ZNRecord configZnRecord = new ZNRecord(_participant.getInstanceName());
    configZnRecord.setSimpleField(TaskStateModel.TASK_JAR_FILE, "src/test/resources/Reindex.jar");
    configZnRecord.setSimpleField(TaskStateModel.TASK_VERSION, "1.0.0");
    List<String> taskClasses = new ArrayList<String>();
    taskClasses.add("com.mycompany.mocktask.MockTask");
    configZnRecord.setListField(TaskStateModel.TASK_CLASSES, taskClasses);
    configZnRecord.setSimpleField(TaskStateModel.TASK_FACTORY, "com.mycompany.mocktask.MockTaskFactory");
    String path = String.format("/%s/%s_Reindex", _clusterName, TaskStateModel.TASK_PATH);
    _participant.getZkClient().create(path, configZnRecord, CreateMode.PERSISTENT);
  }

  @Test
  public void testDynamicTaskLoading() throws Exception {
    String workflowName = TestHelper.getTestMethodName();
    TaskDriver driver = new TaskDriver(_manager);
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

    TaskState finalState = driver.pollForWorkflowState(workflowName, 2000, TaskState.COMPLETED);
    AssertJUnit.assertEquals(finalState, TaskState.COMPLETED);
  }

  @AfterClass
  public void afterClass() throws Exception {
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
