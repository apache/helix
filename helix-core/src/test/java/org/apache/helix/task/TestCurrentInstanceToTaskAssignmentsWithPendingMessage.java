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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import java.util.SortedSet;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestCurrentInstanceToTaskAssignmentsWithPendingMessage {

  /**
   * This is a unit test that tests that all task partitions with currentState or pending message is
   * added to the result of getCurrentInstanceToTaskAssignments method.
   */
  @Test
  public void testCurrentInstanceToTaskAssignmentsWithPendingMessage() {
    Random random = new Random();
    String jobName = "job";
    String nodeName = "localhost";
    int numTasks = 100;

    // Create an Iterable of LiveInstances
    Collection<String> liveInstances = new HashSet<>();
    liveInstances.add("localhost");

    // Create allTaskPartitions
    Set<Integer> allTaskPartitions = new HashSet<>();

    // Create a mock CurrentStateOutput
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);

    // Generate a CurrentStateMap and PendingMessageMap
    Map<Partition, Map<String, String>> currentStateMap = new HashMap<>();
    Map<Partition, Map<String, Message>> pendingMessageMap = new HashMap<>();

    int tasksWithCurrentStateOnly = 0;
    int tasksWithCurrentStateAndPendingMessage = 0;
    int tasksWithPendingMessageOnly = 0;

    List<String> states =
        Arrays.asList(TaskPartitionState.INIT.name(), TaskPartitionState.RUNNING.name(),
            TaskPartitionState.TIMED_OUT.name(), TaskPartitionState.TASK_ERROR.name(),
            TaskPartitionState.COMPLETED.name(), TaskPartitionState.STOPPED.name(),
            TaskPartitionState.TASK_ABORTED.name(), TaskPartitionState.DROPPED.name());

    for (int i = 0; i < numTasks; i++) {
      allTaskPartitions.add(i);
      Partition task = new Partition(jobName + "_" + i);
      currentStateMap.put(task, new HashMap<>());
      pendingMessageMap.put(task, new HashMap<>());

      String currentState = states.get(random.nextInt(states.size()));
      Message message = new Message(Message.MessageType.STATE_TRANSITION, "12345");
      message.setToState(states.get(random.nextInt(states.size())));
      message.setFromState(states.get(random.nextInt(states.size())));

      int randInt = random.nextInt(4);
      if (randInt == 0) {
        tasksWithCurrentStateOnly = tasksWithCurrentStateOnly + 1;
        currentStateMap.get(task).put(nodeName, currentState);
      } else if (randInt == 1) {
        tasksWithCurrentStateAndPendingMessage = tasksWithCurrentStateAndPendingMessage + 1;
        currentStateMap.get(task).put(nodeName, currentState);
        pendingMessageMap.get(task).put(nodeName, message);
      } else if (randInt == 2) {
        tasksWithPendingMessageOnly = tasksWithPendingMessageOnly + 1;
        pendingMessageMap.get(task).put(nodeName, message);
      }
    }

    when(currentStateOutput.getCurrentStateMap(jobName)).thenReturn(currentStateMap);
    when(currentStateOutput.getPendingMessageMap(jobName)).thenReturn(pendingMessageMap);

    // Create an empty tasksToDrop
    Map<String, Set<Integer>> tasksToDrop = new HashMap<>();

    // Call the static method we are testing
    Map<String, SortedSet<Integer>> result = JobDispatcher.getCurrentInstanceToTaskAssignments(
        liveInstances, currentStateOutput, jobName, tasksToDrop);
    Assert.assertEquals(result.get(nodeName).size(), (tasksWithCurrentStateOnly
        + tasksWithCurrentStateAndPendingMessage + tasksWithPendingMessageOnly));
  }
}
