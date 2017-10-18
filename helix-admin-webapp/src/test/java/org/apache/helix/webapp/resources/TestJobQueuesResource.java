package org.apache.helix.webapp.resources;

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

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.beans.JobBean;
import org.apache.helix.task.beans.WorkflowBean;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.webapp.AdminTestBase;
import org.apache.helix.webapp.AdminTestHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.yaml.snakeyaml.Yaml;

public class TestJobQueuesResource extends AdminTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestJobQueuesResource.class);

  @Test
  public void test() throws Exception {

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;
    final int p = 20;
    final int r = 3;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    _gSetupTool.addCluster(clusterName, true);
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      _gSetupTool.addInstanceToCluster(clusterName, instanceName);
    }

    // Set up target db
    _gSetupTool.addResourceToCluster(clusterName, WorkflowGenerator.DEFAULT_TGT_DB, p,
        "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(clusterName, WorkflowGenerator.DEFAULT_TGT_DB, r);

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("DummyTask", new TaskFactory() {
      @Override
      public Task createNewTask(TaskCallbackContext context) {
        return new MockTask(context);
      }
    });

    // Start dummy participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);

      // Register a Task state model factory.
      StateMachineEngine stateMachine = participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory("Task", new TaskStateModelFactory(participants[i],
          taskFactoryReg));
      participants[i].syncStart();
    }

    // start controller
    String controllerName = "controller";
    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, controllerName);
    controller.syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // Start a queue
    String queueName = "myQueue1";
    LOG.info("Starting job-queue: " + queueName);
    String jobQueueYamlConfig = "name: " + queueName;

    String resourceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues";
    ZNRecord postRet = AdminTestHelper.post(_gClient, resourceUrl, jobQueueYamlConfig);
    LOG.info("Started job-queue: " + queueName + ", ret: " + postRet);

    LOG.info("Getting all job-queues");
    ZNRecord getRet = AdminTestHelper.get(_gClient, resourceUrl);
    LOG.info("Got job-queues: " + getRet);

    // Enqueue job
    resourceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues/" + queueName;

    WorkflowBean wfBean = new WorkflowBean();
    wfBean.name = queueName;

    JobBean jBean1 = new JobBean();
    jBean1.name = "myJob1";
    jBean1.command = "DummyTask";
    jBean1.targetResource = WorkflowGenerator.DEFAULT_TGT_DB;
    jBean1.targetPartitionStates = Lists.newArrayList("MASTER");

    JobBean jBean2 = new JobBean();
    jBean2.name = "myJob2";
    jBean2.command = "DummyTask";
    jBean2.targetResource = WorkflowGenerator.DEFAULT_TGT_DB;
    jBean2.targetPartitionStates = Lists.newArrayList("SLAVE");

    wfBean.jobs = Lists.newArrayList(jBean1, jBean2);
    String jobYamlConfig = new Yaml().dump(wfBean);
    LOG.info("Enqueuing jobs: " + jobQueueYamlConfig);

    Map<String, String> paraMap = new HashMap<String, String>();
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, TaskDriver.DriverCommand.start.toString());

    String postBody =
        String.format("%s=%s&%s=%s", JsonParameters.JSON_PARAMETERS,
            ClusterRepresentationUtil.ObjectToJson(paraMap), ResourceUtil.YamlParamKey.NEW_JOB.toString(),
            jobYamlConfig);
    postRet = AdminTestHelper.post(_gClient, resourceUrl, postBody);
    LOG.info("Enqueued job, ret: " + postRet);

    // Get job
    resourceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues/" + queueName
            + "/" + jBean1.name;
    getRet = AdminTestHelper.get(_gClient, resourceUrl);
    LOG.info("Got job: " + getRet);

    // Stop job queue
    resourceUrl =
            "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues/" + queueName;
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, TaskDriver.DriverCommand.stop.toString());
    postBody = String.format("%s=%s", JsonParameters.JSON_PARAMETERS, ClusterRepresentationUtil.ObjectToJson(paraMap));
    postRet = AdminTestHelper.post(_gClient, resourceUrl, postBody);
    LOG.info("Stopped job-queue, ret: " + postRet);

    // Delete a job
    resourceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues/" + queueName
            + "/" + jBean2.name;
    AdminTestHelper.delete(_gClient, resourceUrl);
    LOG.info("Delete a job: ");

    // Resume job queue
    resourceUrl =
        "http://localhost:" + ADMIN_PORT + "/clusters/" + clusterName + "/jobQueues/" + queueName;
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, TaskDriver.DriverCommand.resume.toString());
    postBody = String.format("%s=%s", JsonParameters.JSON_PARAMETERS, ClusterRepresentationUtil.ObjectToJson(paraMap));
    postRet = AdminTestHelper.post(_gClient, resourceUrl, postBody);
    LOG.info("Resumed job-queue, ret: " + postRet);

    // Flush job queue
    paraMap.put(JsonParameters.MANAGEMENT_COMMAND, "flush");
    postBody =
        JsonParameters.JSON_PARAMETERS + "=" + ClusterRepresentationUtil.ObjectToJson(paraMap);
    postRet = AdminTestHelper.post(_gClient, resourceUrl, postBody);
    LOG.info("Flushed job-queue, ret: " + postRet);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      if (participants[i] != null && participants[i].isConnected()) {
        participants[i].syncStop();
      }
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
