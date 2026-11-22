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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The available thread capacity (published as the AvailableThreadGauge metric) must count tasks
 * that are already INIT/RUNNING on participants when the AssignableInstanceManager is rebuilt from
 * current state.
 */
public class TestAssignableInstanceManagerAvailableCapacity {
  private static final String CLUSTER = "TestCluster_0";
  private static final String JOB = "Job_0";
  private static final int THREADS_PER_INSTANCE = 10;
  private static final int TOTAL_THREADS = 20;
  private static final String[] INSTANCES = {"Instance_0", "Instance_1"};

  private AssignableInstanceManager buildFromCurrentState(CurrentStateOutput currentStateOutput) {
    Map<String, LiveInstance> liveInstances = new HashMap<>();
    Map<String, InstanceConfig> instanceConfigs = new HashMap<>();
    for (String instance : INSTANCES) {
      LiveInstance liveInstance = new LiveInstance(instance);
      liveInstance.setCurrentTaskThreadPoolSize(THREADS_PER_INSTANCE);
      liveInstances.put(instance, liveInstance);
      instanceConfigs.put(instance, new InstanceConfig(instance));
    }
    TaskDataCache taskDataCache = mock(TaskDataCache.class);
    when(taskDataCache.getJobConfigMap()).thenReturn(Collections.emptyMap());

    Resource resource = new Resource(JOB);
    resource.setStateModelDefRef(TaskConstants.STATE_MODEL_NAME);

    AssignableInstanceManager manager = new AssignableInstanceManager();
    manager.buildAssignableInstancesFromCurrentState(new ClusterConfig(CLUSTER), taskDataCache,
        liveInstances, instanceConfigs, currentStateOutput,
        Collections.singletonMap(JOB, resource));
    return manager;
  }

  private static int availableDefault(AssignableInstanceManager manager) {
    return manager.getAvailableThreadCapacityMap().get(AssignableInstance.DEFAULT_QUOTA_TYPE);
  }

  @Test
  public void testRunningTasksAreNotAvailable() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (int i = 0; i < 3; i++) {
      currentStateOutput.setCurrentState(JOB, new Partition(JOB + "_" + i), INSTANCES[0],
          TaskPartitionState.RUNNING.name());
    }
    currentStateOutput.setCurrentState(JOB, new Partition(JOB + "_3"), INSTANCES[1],
        TaskPartitionState.INIT.name());

    AssignableInstanceManager manager = buildFromCurrentState(currentStateOutput);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS - 4);

    // Releasing a running task frees exactly one thread, releasing it again frees nothing
    TaskConfig runningTask = new TaskConfig(null, null, JOB + "_0", null);
    manager.release(INSTANCES[0], runningTask, null);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS - 3);
    manager.release(INSTANCES[0], runningTask, null);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS - 3);
  }

  @Test
  public void testFullCluster() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    int partition = 0;
    for (String instance : INSTANCES) {
      for (int i = 0; i < THREADS_PER_INSTANCE; i++) {
        currentStateOutput.setCurrentState(JOB, new Partition(JOB + "_" + partition++), instance,
            TaskPartitionState.RUNNING.name());
      }
    }
    AssignableInstanceManager manager = buildFromCurrentState(currentStateOutput);
    Assert.assertEquals(availableDefault(manager), 0);
  }

  @Test
  public void testReleasingFinishedTaskDoesNotAddCapacity() {
    // A COMPLETED task does not occupy a thread, so it is not restored. The task pipeline still
    // calls release() for it until it is dropped.
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(JOB, new Partition(JOB + "_0"), INSTANCES[0],
        TaskPartitionState.COMPLETED.name());
    AssignableInstanceManager manager = buildFromCurrentState(currentStateOutput);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS);

    manager.release(INSTANCES[0], new TaskConfig(null, null, JOB + "_0", null), null);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS);
  }

  @Test
  public void testAssignAndReleaseAfterBuild() {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setCurrentState(JOB, new Partition(JOB + "_0"), INSTANCES[0],
        TaskPartitionState.RUNNING.name());
    AssignableInstanceManager manager = buildFromCurrentState(currentStateOutput);

    TaskConfig newTask = new TaskConfig(null, null, JOB + "_1", null);
    TaskAssignResult result = manager.tryAssign(INSTANCES[1], newTask, null);
    Assert.assertTrue(result.isSuccessful());
    manager.assign(INSTANCES[1], result);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS - 2);

    manager.release(INSTANCES[1], newTask, null);
    Assert.assertEquals(availableDefault(manager), TOTAL_THREADS - 1);
  }
}
