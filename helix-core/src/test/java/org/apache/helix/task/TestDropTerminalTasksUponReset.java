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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.Partition;
import org.apache.helix.model.ResourceAssignment;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;

public class TestDropTerminalTasksUponReset {

  /**
   * This is a unit test that tests that all task partitions with requested state = DROPPED will be
   * added to tasksToDrop.
   */
  @Test
  public void testDropAllTerminalTasksUponReset() {
    Random random = new Random();
    String jobName = "job";
    String nodeName = "localhost";
    int numTasks = 10;

    // Create an Iterable of LiveInstances
    Collection<String> liveInstances = new HashSet<>();
    liveInstances.add("localhost");

    // Create a dummy ResourceAssignment
    ResourceAssignment prevAssignment = new ResourceAssignment(jobName);

    // Create allTaskPartitions
    Set<Integer> allTaskPartitions = new HashSet<>();

    // Create a mock CurrentStateOutput
    CurrentStateOutput currentStateOutput = mock(CurrentStateOutput.class);

    // Generate a CurrentStateMap
    Map<Partition, Map<String, String>> currentStateMap = new HashMap<>();
    when(currentStateOutput.getCurrentStateMap(jobName)).thenReturn(currentStateMap);

    for (int i = 0; i < numTasks; i++) {
      allTaskPartitions.add(i);
      Partition task = new Partition(jobName + "_" + i);
      currentStateMap.put(task, new HashMap<>());

      // Pick some random currentState between COMPLETED and TASK_ERROR
      String currentState = (random.nextBoolean()) ? TaskPartitionState.COMPLETED.name()
          : TaskPartitionState.TASK_ERROR.name();

      // First half of the tasks to be dropped on each instance
      if (i < numTasks / 2) {
        // requested state is DROPPED
        currentStateMap.get(task).put("localhost", currentState);
        when(currentStateOutput.getRequestedState(jobName, task, nodeName))
            .thenReturn(TaskPartitionState.DROPPED.name());
      } else {
        // requested state is nothing
        when(currentStateOutput.getRequestedState(jobName, task, nodeName)).thenReturn(null);
      }
    }

    // Create an empty tasksToDrop
    Map<String, Set<Integer>> tasksToDrop = new HashMap<>();

    // Call the static method we are testing
    JobDispatcher.getPrevInstanceToTaskAssignments(liveInstances, prevAssignment, allTaskPartitions,
        currentStateOutput, jobName, tasksToDrop);

    // Check that tasksToDrop has (numTasks / 2) partitions as we intended regardless of what the
    // current states of the tasks were
    Assert.assertEquals(numTasks / 2, tasksToDrop.get(nodeName).size());
  }
}
