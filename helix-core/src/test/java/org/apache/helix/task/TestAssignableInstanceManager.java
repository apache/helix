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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.task.assigner.AssignableInstance;
import org.apache.helix.task.assigner.TaskAssignResult;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestAssignableInstanceManager {
  private static final int NUM_PARTICIPANTS = 3;
  private static final int NUM_JOBS = 3;
  private static final int NUM_TASKS = 3;
  private static final String CLUSTER_NAME = "TestCluster_0";
  private static final String INSTANCE_PREFIX = "Instance_";
  private static final String JOB_PREFIX = "Job_";
  private static final String TASK_PREFIX = "Task_";

  private ClusterConfig _clusterConfig;
  private MockTaskDataCache _taskDataCache;
  private AssignableInstanceManager _assignableInstanceManager;
  private Map<String, LiveInstance> _liveInstances;
  private Map<String, InstanceConfig> _instanceConfigs;
  private Set<String> _taskIDs; // To keep track of what tasks were created

  @BeforeClass
  public void beforeClass() {
    System.out.println(
        "START " + this.getClass().getSimpleName() + " at " + new Date(System.currentTimeMillis()));
    _clusterConfig = new ClusterConfig(CLUSTER_NAME);
    _taskDataCache = new MockTaskDataCache(CLUSTER_NAME);
    _liveInstances = new HashMap<>();
    _instanceConfigs = new HashMap<>();
    _taskIDs = new HashSet<>();

    // Populate live instances and their corresponding instance configs
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = INSTANCE_PREFIX + i;
      LiveInstance liveInstance = new LiveInstance(instanceName);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      _liveInstances.put(instanceName, liveInstance);
      _instanceConfigs.put(instanceName, instanceConfig);
    }

    // Populate taskDataCache with JobConfigs and JobContexts
    for (int i = 0; i < NUM_JOBS; i++) {
      String jobName = JOB_PREFIX + i;

      // Create a JobConfig
      JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
      List<TaskConfig> taskConfigs = new ArrayList<>();
      for (int j = 0; j < NUM_TASKS; j++) {
        String taskID = jobName + "_" + TASK_PREFIX + j;
        TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
        taskConfigBuilder.setTaskId(taskID);
        _taskIDs.add(taskID);
        taskConfigs.add(taskConfigBuilder.build());
      }

      jobConfigBuilder.setJobId(jobName);
      jobConfigBuilder.addTaskConfigs(taskConfigs);
      jobConfigBuilder.setCommand("MOCK");
      jobConfigBuilder.setWorkflow("WORKFLOW");
      _taskDataCache.addJobConfig(jobName, jobConfigBuilder.build());

      // Create a JobContext
      ZNRecord znRecord = new ZNRecord(JOB_PREFIX + "context_" + i);
      JobContext jobContext = new MockJobContext(znRecord, _liveInstances, _taskIDs);
      _taskDataCache.addJobContext(jobName, jobContext);
      _taskIDs.clear();
    }

    // Create an AssignableInstanceManager and build
    _assignableInstanceManager = new AssignableInstanceManager();
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, _taskDataCache,
        _liveInstances, _instanceConfigs);
  }

  @Test
  public void testGetAssignableInstanceMap() {
    Map<String, AssignableInstance> assignableInstanceMap =
        _assignableInstanceManager.getAssignableInstanceMap();
    for (String liveInstance : _liveInstances.keySet()) {
      Assert.assertTrue(assignableInstanceMap.containsKey(liveInstance));
    }
  }

  @Test
  public void testGetTaskAssignResultMap() {
    Map<String, TaskAssignResult> taskAssignResultMap =
        _assignableInstanceManager.getTaskAssignResultMap();
    for (String taskID : _taskIDs) {
      Assert.assertTrue(taskAssignResultMap.containsKey(taskID));
    }
  }

  @Test
  public void testUpdateAssignableInstances() {
    Map<String, LiveInstance> newLiveInstances = new HashMap<>();
    Map<String, InstanceConfig> newInstanceConfigs = new HashMap<>();

    // A brand new set of LiveInstances
    for (int i = NUM_PARTICIPANTS; i < NUM_PARTICIPANTS + 3; i++) {
      String instanceName = INSTANCE_PREFIX + i;
      newLiveInstances.put(instanceName, new LiveInstance(instanceName));
      newInstanceConfigs.put(instanceName, new InstanceConfig(instanceName));
    }

    _assignableInstanceManager.updateAssignableInstances(_clusterConfig, newLiveInstances,
        newInstanceConfigs);

    // Check that the assignable instance map contains new instances and there are no
    // TaskAssignResults due to previous live instances being removed
    Assert.assertEquals(_assignableInstanceManager.getTaskAssignResultMap().size(), 0);
    Assert.assertEquals(_assignableInstanceManager.getAssignableInstanceMap().size(),
        newLiveInstances.size());
    for (String instance : newLiveInstances.keySet()) {
      Assert
          .assertTrue(_assignableInstanceManager.getAssignableInstanceMap().containsKey(instance));
    }
  }

  public class MockTaskDataCache extends TaskDataCache {
    private Map<String, JobConfig> _jobConfigMap;
    private Map<String, WorkflowConfig> _workflowConfigMap;
    private Map<String, JobContext> _jobContextMap;
    private Map<String, WorkflowContext> _workflowContextMap;

    public MockTaskDataCache(String clusterName) {
      super(clusterName);
      _jobConfigMap = new HashMap<>();
      _workflowConfigMap = new HashMap<>();
      _jobContextMap = new HashMap<>();
      _workflowContextMap = new HashMap<>();
    }

    public void addJobConfig(String jobName, JobConfig jobConfig) {
      _jobConfigMap.put(jobName, jobConfig);
    }

    public void addJobContext(String jobName, JobContext jobContext) {
      _jobContextMap.put(jobName, jobContext);
    }

    public void addWorkflowConfig(String workflowName, WorkflowConfig workflowConfig) {
      _workflowConfigMap.put(workflowName, workflowConfig);
    }

    public void addWorkflowContext(String workflowName, WorkflowContext workflowContext) {
      _workflowContextMap.put(workflowName, workflowContext);
    }

    @Override
    public JobContext getJobContext(String jobName) {
      return _jobContextMap.get(jobName);
    }

    @Override
    public Map<String, JobConfig> getJobConfigMap() {
      return _jobConfigMap;
    }

    @Override
    public Map<String, WorkflowConfig> getWorkflowConfigMap() {
      return _workflowConfigMap;
    }

    public Map<String, JobContext> getJobContextMap() {
      return _jobContextMap;
    }

    public Map<String, WorkflowContext> getWorkflowContextMap() {
      return _workflowContextMap;
    }
  }

  public class MockJobContext extends JobContext {
    private Set<Integer> _taskPartitionSet;
    private Map<Integer, TaskPartitionState> _taskPartitionStateMap;
    private Map<Integer, String> _partitionToTaskIDMap;
    private Map<Integer, String> _taskToInstanceMap;

    public MockJobContext(ZNRecord record, Map<String, LiveInstance> liveInstanceMap,
        Set<String> taskIDs) {
      super(record);
      _taskPartitionSet = new HashSet<>();
      _taskPartitionStateMap = new HashMap<>();
      _partitionToTaskIDMap = new HashMap<>();
      _taskToInstanceMap = new HashMap<>();

      List<String> taskIDList = new ArrayList<>(taskIDs);
      for (int i = 0; i < taskIDList.size(); i++) {
        _taskPartitionSet.add(i);
        _taskPartitionStateMap.put(i, TaskPartitionState.RUNNING);
        _partitionToTaskIDMap.put(i, taskIDList.get(i));
        String someInstance = liveInstanceMap.keySet().iterator().next();
        _taskToInstanceMap.put(i, someInstance);
      }
    }

    @Override
    public Set<Integer> getPartitionSet() {
      return _taskPartitionSet;
    }

    @Override
    public TaskPartitionState getPartitionState(int p) {
      return _taskPartitionStateMap.get(p);
    }

    @Override
    public String getAssignedParticipant(int p) {
      return _taskToInstanceMap.get(p);
    }

    @Override
    public String getTaskIdForPartition(int p) {
      return _partitionToTaskIDMap.get(p);
    }
  }
}