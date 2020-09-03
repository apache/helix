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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.Partition;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestTargetedTaskStateChange {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_PREFIX = "Instance_";
  private static final int NUM_PARTICIPANTS = 3;
  private static final String WORKFLOW_NAME = "TestWorkflow";
  private static final String JOB_NAME = "TestJob";
  private static final String PARTITION_NAME = "0";
  private static final String TARGET_RESOURCES = "TestDB";
  private Map<String, LiveInstance> _liveInstances;
  private Map<String, InstanceConfig> _instanceConfigs;
  private ClusterConfig _clusterConfig;
  private AssignableInstanceManager _assignableInstanceManager;

  @BeforeClass
  public void beforeClass() {
    // Populate live instances and their corresponding instance configs
    _liveInstances = new HashMap<>();
    _instanceConfigs = new HashMap<>();
    _clusterConfig = new ClusterConfig(CLUSTER_NAME);
    for (int i = 0; i < NUM_PARTICIPANTS; i++) {
      String instanceName = INSTANCE_PREFIX + i;
      LiveInstance liveInstance = new LiveInstance(instanceName);
      InstanceConfig instanceConfig = new InstanceConfig(instanceName);
      _liveInstances.put(instanceName, liveInstance);
      _instanceConfigs.put(instanceName, instanceConfig);
    }
    _assignableInstanceManager = new AssignableInstanceManager();
  }

  /**
   * This test checks the behaviour of the controller while there are two current states for two
   * different instances.
   * Scenario:
   * Instance0: Slave, Instance1: Master, Instance2: Slave
   * CurrentState: Instance0: Running, Instance1: Running
   * Expected paMap: Instance0 -> Dropped
   */
  @Test
  public void testTwoRunningCurrentStates() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    when(mock._cache.getInstanceConfigMap()).thenReturn(_instanceConfigs);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _instanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateOrMessageChange()).thenReturn(true);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext, mock._currentStateOutput, bestPossibleStateOutput);
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    Assert.assertEquals(TaskPartitionState.DROPPED.name(), bestPossibleStateOutput
        .getPartitionStateMap(JOB_NAME).getPartitionMap(taskPartition).get(INSTANCE_PREFIX + "0"));
  }

  /**
   * This test checks the behaviour of the controller while there is one current state.
   * Scenario:
   * Instance0: Slave, Instance1: Master, Instance2: Slave
   * CurrentState: Instance0: Running
   * Expected paMap: Instance1 -> Running
   */
  @Test
  public void testOneRunningOneNull() {
    MockTestInformation mock = new MockTestInformation();
    when(mock._cache.getWorkflowConfig(WORKFLOW_NAME)).thenReturn(mock._workflowConfig);
    when(mock._cache.getJobConfig(JOB_NAME)).thenReturn(mock._jobConfig);
    when(mock._cache.getTaskDataCache()).thenReturn(mock._taskDataCache);
    when(mock._cache.getJobContext(JOB_NAME)).thenReturn(mock._jobContext);
    when(mock._cache.getIdealStates()).thenReturn(mock._idealStates);
    when(mock._cache.getEnabledLiveInstances()).thenReturn(_liveInstances.keySet());
    when(mock._cache.getInstanceConfigMap()).thenReturn(_instanceConfigs);
    when(mock._cache.getClusterConfig()).thenReturn(_clusterConfig);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME)).thenReturn(mock._runtimeJobDag);
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig, mock._taskDataCache,
        _liveInstances, _instanceConfigs);
    when(mock._cache.getAssignableInstanceManager()).thenReturn(_assignableInstanceManager);
    when(mock._cache.getExistsLiveInstanceOrCurrentStateOrMessageChange()).thenReturn(false);
    Set<String> inflightJobDag = new HashSet<>();
    inflightJobDag.add(JOB_NAME);
    when(mock._taskDataCache.getRuntimeJobDag(WORKFLOW_NAME).getInflightJobList())
        .thenReturn(inflightJobDag);
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    WorkflowDispatcher workflowDispatcher = new WorkflowDispatcher();
    workflowDispatcher.updateCache(mock._cache);
    workflowDispatcher.updateWorkflowStatus(WORKFLOW_NAME, mock._workflowConfig,
        mock._workflowContext, mock._currentStateOutput2, bestPossibleStateOutput);
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    Assert.assertEquals(TaskPartitionState.RUNNING.name(), bestPossibleStateOutput
        .getPartitionStateMap(JOB_NAME).getPartitionMap(taskPartition).get(INSTANCE_PREFIX + "1"));
  }

  private WorkflowConfig prepareWorkflowConfig() {
    WorkflowConfig.Builder workflowConfigBuilder = new WorkflowConfig.Builder();
    workflowConfigBuilder.setWorkflowId(WORKFLOW_NAME);
    workflowConfigBuilder.setTerminable(false);
    workflowConfigBuilder.setTargetState(TargetState.START);
    workflowConfigBuilder.setJobQueue(true);
    JobDag jobDag = new JobDag();
    jobDag.addNode(JOB_NAME);
    workflowConfigBuilder.setJobDag(jobDag);
    return workflowConfigBuilder.build();
  }

  private JobConfig prepareJobConfig() {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setWorkflow(WORKFLOW_NAME);
    jobConfigBuilder.setCommand("TestCommand");
    jobConfigBuilder.setTargetResource(TARGET_RESOURCES);
    jobConfigBuilder.setJobId(JOB_NAME);
    List<String> targetPartition = new ArrayList<>();
    targetPartition.add(TARGET_RESOURCES + "_0");
    jobConfigBuilder.setTargetPartitions(targetPartition);
    Set<String> targetPartitionStates = new HashSet<>();
    targetPartitionStates.add("MASTER");
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
    taskConfigBuilder.setTaskId("0");
    taskConfigs.add(taskConfigBuilder.build());
    jobConfigBuilder.setTargetPartitionStates(targetPartitionStates);
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    JobConfig jobConfig = jobConfigBuilder.build();
    return jobConfig;
  }

  private WorkflowContext prepareWorkflowContext() {
    ZNRecord record = new ZNRecord(WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.StartTime.name(), "0");
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.NAME.name(), WORKFLOW_NAME);
    record.setSimpleField(WorkflowContext.WorkflowContextProperties.STATE.name(),
        TaskState.IN_PROGRESS.name());
    Map<String, String> jobState = new HashMap<>();
    jobState.put(JOB_NAME, TaskState.IN_PROGRESS.name());
    record.setMapField(WorkflowContext.WorkflowContextProperties.JOB_STATES.name(), jobState);
    return new WorkflowContext(record);
  }

  private JobContext prepareJobContext(String instance) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    JobContext jobContext = new JobContext(record);
    jobContext.setStartTime(0L);
    jobContext.setName(JOB_NAME);
    jobContext.setStartTime(0L);
    jobContext.setPartitionState(0, TaskPartitionState.RUNNING);
    jobContext.setPartitionTarget(0, instance);
    jobContext.setPartitionTarget(0, TARGET_RESOURCES + "_0");
    return jobContext;
  }

  private Map<String, IdealState> prepareIdealStates(String instance1, String instance2,
      String instance3) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    record.setSimpleField(IdealState.IdealStateProperty.NUM_PARTITIONS.name(), "1");
    record.setSimpleField(IdealState.IdealStateProperty.EXTERNAL_VIEW_DISABLED.name(), "true");
    record.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(), "AUTO");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "TASK");
    record.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "1");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "Task");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_FACTORY_NAME.name(), "DEFAULT");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
        "org.apache.helix.task.JobRebalancer");
    record.setMapField(JOB_NAME + "_" + PARTITION_NAME, new HashMap<>());
    record.setListField(JOB_NAME + "_" + PARTITION_NAME, new ArrayList<>());
    Map<String, IdealState> idealStates = new HashMap<>();
    idealStates.put(JOB_NAME, new IdealState(record));

    ZNRecord recordDB = new ZNRecord(TARGET_RESOURCES);
    recordDB.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "3");
    recordDB.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "FULL_AUTO");
    record.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(), "AUTO_REBALANCE");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(), "MasterSlave");
    record.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(),
        "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    record.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
        "org.apache.helix.controller.rebalancer.DelayedAutoRebalancer");
    Map<String, String> mapping = new HashMap<>();
    mapping.put(instance1, "MASTER");
    mapping.put(instance2, "SLAVE");
    mapping.put(instance3, "SLAVE");
    recordDB.setMapField(TARGET_RESOURCES + "_0", mapping);
    List<String> listField = new ArrayList<>();
    listField.add(instance1);
    listField.add(instance2);
    listField.add(instance3);
    recordDB.setListField(TARGET_RESOURCES + "_0", listField);
    idealStates.put(TARGET_RESOURCES, new IdealState(recordDB));

    return idealStates;
  }

  private CurrentStateOutput prepareCurrentState(String masterInstance, String slaveInstance,
      String masterState, String slaveState) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    currentStateOutput.setEndTime(JOB_NAME, taskPartition, masterInstance, 0L);
    currentStateOutput.setEndTime(JOB_NAME, taskPartition, slaveInstance, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition, masterInstance, masterState);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition, slaveInstance, slaveState);
    currentStateOutput.setInfo(JOB_NAME, taskPartition, masterInstance, "");
    currentStateOutput.setInfo(JOB_NAME, taskPartition, slaveInstance, "");
    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);
    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, masterInstance, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, masterInstance, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, masterInstance, "");
    return currentStateOutput;
  }

  private CurrentStateOutput prepareCurrentState2(String masterInstance, String masterState) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    currentStateOutput.setBucketSize(JOB_NAME, 0);
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    currentStateOutput.setEndTime(JOB_NAME, taskPartition, masterInstance, 0L);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition, masterInstance, masterState);
    currentStateOutput.setInfo(JOB_NAME, taskPartition, masterInstance, "");
    currentStateOutput.setResourceStateModelDef(TARGET_RESOURCES, "MasterSlave");
    currentStateOutput.setBucketSize(TARGET_RESOURCES, 0);
    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, masterInstance, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, masterInstance, "MASTER");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, masterInstance, "");
    return currentStateOutput;
  }

  private class MockTestInformation {
    private static final String SLAVE_INSTANCE = INSTANCE_PREFIX + "0";
    private static final String MASTER_INSTANCE = INSTANCE_PREFIX + "1";
    private static final String SLAVE_INSTANCE_2 = INSTANCE_PREFIX + "2";

    private WorkflowControllerDataProvider _cache = mock(WorkflowControllerDataProvider.class);
    private WorkflowConfig _workflowConfig = prepareWorkflowConfig();
    private WorkflowContext _workflowContext = prepareWorkflowContext();
    private Map<String, IdealState> _idealStates =
        prepareIdealStates(MASTER_INSTANCE, SLAVE_INSTANCE, SLAVE_INSTANCE_2);
    private JobConfig _jobConfig = prepareJobConfig();
    private JobContext _jobContext = prepareJobContext(SLAVE_INSTANCE);
    private CurrentStateOutput _currentStateOutput = prepareCurrentState(MASTER_INSTANCE,
        SLAVE_INSTANCE, TaskPartitionState.RUNNING.name(), TaskPartitionState.RUNNING.name());
    private CurrentStateOutput _currentStateOutput2 =
        prepareCurrentState2(MASTER_INSTANCE, TaskPartitionState.RUNNING.name());
    private TaskDataCache _taskDataCache = mock(TaskDataCache.class);
    private RuntimeJobDag _runtimeJobDag = mock(RuntimeJobDag.class);

    MockTestInformation() {
    }
  }
}
