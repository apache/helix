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
    taskFactoryReg.put("mytask1", new TaskFactory() {
      @Override
      public Task createNewTask(String config) {
        return new MyTask(1);
      }
    });
    taskFactoryReg.put("mytask2", new TaskFactory() {
      @Override
      public Task createNewTask(String config) {
        return new MyTask(2);
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
    taskManager.addTaskToQueue("mytask1", "myqueue");
    Thread.sleep(5000);
    taskManager.addTaskToQueue("mytask2", "myqueue");
    taskManager.cancelTask("myqueue", "mytask1");

    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
  }

  public static class MyTask implements Task {
    private final int _id;
    private Thread _t;
    private TaskResult.Status _status = null;

    public MyTask(int id) {
      _id = id;
    }

    @Override
    public TaskResult run() {
      _t = new Thread() {
        @Override
        public void run() {
          try {
            Thread.sleep(60000);
            _status = TaskResult.Status.COMPLETED;
            System.err.println("task complete for " + _id);
          } catch (InterruptedException e) {
            _status = TaskResult.Status.CANCELED;
            System.err.println("task canceled for " + _id);
            interrupt();
          }
        }
      };
      _t.start();
      try {
        _t.join();
      } catch (InterruptedException e) {
        _status = TaskResult.Status.CANCELED;
      }
      return new TaskResult(_status, "");
    }

    @Override
    public void cancel() {
      if (_t != null && _t.isAlive()) {
        _t.interrupt();
      }
    }
  }
}
