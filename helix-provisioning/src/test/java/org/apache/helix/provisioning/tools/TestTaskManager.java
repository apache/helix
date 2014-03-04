package org.apache.helix.provisioning.tools;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixConnection;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.TestHelixConnection;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZkHelixConnection;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.TaskStateModelFactory;
import org.testng.annotations.Test;

public class TestTaskManager extends ZkUnitTestBase {
  @Test
  public void testBasic() throws Exception {
    final int NUM_PARTICIPANTS = 3;
    final int NUM_PARTITIONS = 1;
    final int NUM_REPLICAS = 1;

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    // Set up cluster
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestService", // resource name prefix
        1, // resources
        NUM_PARTITIONS, // partitions per resource
        NUM_PARTICIPANTS, // number of nodes
        NUM_REPLICAS, // replicas
        "StatelessService", RebalanceMode.FULL_AUTO, // just get everything up
        true); // do rebalance

    Map<String, TaskFactory> taskFactoryReg = new HashMap<String, TaskFactory>();
    taskFactoryReg.put("myqueue", new TaskFactory() {
      @Override
      public Task createNewTask(String config) {
        return new MyTask();
      }
    });
    MockParticipantManager[] participants = new MockParticipantManager[NUM_PARTICIPANTS];
    for (int i = 0; i < participants.length; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].getStateMachineEngine()
          .registerStateModelFactory(StateModelDefId.from("StatelessService"),
              new TestHelixConnection.MockStateModelFactory());
      participants[i].getStateMachineEngine().registerStateModelFactory(
          StateModelDefId.from("Task"), new TaskStateModelFactory(participants[i], taskFactoryReg));
      participants[i].syncStart();
    }

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_1");
    controller.syncStart();

    HelixConnection connection = new ZkHelixConnection(ZK_ADDR);
    connection.connect();
    ClusterId clusterId = ClusterId.from(clusterName);
    TaskManager taskManager = new TaskManager(clusterId, connection);
    taskManager.createTaskQueue("myqueue", true);
    taskManager.addTaskToQueue("mytask", "myqueue");
    taskManager.addTaskToQueue("mytask2", "myqueue");

    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
  }

  public static class MyTask implements Task {
    @Override
    public TaskResult run() {
      try {
        Thread.sleep(10000);
      } catch (InterruptedException e) {
      }
      System.err.println("task complete");
      return new TaskResult(TaskResult.Status.COMPLETED, "");
    }

    @Override
    public void cancel() {
    }
  }
}
