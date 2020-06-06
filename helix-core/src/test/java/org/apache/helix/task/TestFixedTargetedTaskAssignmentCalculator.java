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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.SortedSet;
import org.apache.helix.common.caches.TaskDataCache;
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

/**
 * This unit test makes sure that FixedTargetedTaskAssignmentCalculator makes correct decision for
 * targeted jobs
 */
public class TestFixedTargetedTaskAssignmentCalculator {
  private static final String CLUSTER_NAME = "TestCluster";
  private static final String INSTANCE_PREFIX = "Instance_";
  private static final int NUM_PARTICIPANTS = 3;

  private static final String WORKFLOW_NAME = "TestWorkflow";
  private static final String JOB_NAME = "TestJob";
  private static final String PARTITION_NAME = "0";
  private static final String TARGET_PARTITION_NAME = "0";
  private static final int PARTITION_ID = 0;
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
  }

  /**
   * Test FixedTargetTaskAssignmentCalculator and make sure that if a job has been assigned
   * before and target partition is still on the same instance and in RUNNING state,
   * we do not make new assignment for that task.
   */
  @Test
  public void testFixedTargetTaskAssignmentCalculatorSameInstanceRunningTask() {
    _assignableInstanceManager = new AssignableInstanceManager();
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig,
        new TaskDataCache("CLUSTER_NAME"), _liveInstances, _instanceConfigs);
    // Preparing the inputs
    String masterInstance = INSTANCE_PREFIX + 1;
    String slaveInstance1 = INSTANCE_PREFIX + 2;
    String slaveInstance2 = INSTANCE_PREFIX + 3;
    CurrentStateOutput currentStateOutput = prepareCurrentState(TaskPartitionState.RUNNING,
        masterInstance, slaveInstance1, slaveInstance2);
    List<String> instances =
        new ArrayList<String>(Arrays.asList(masterInstance, slaveInstance1, slaveInstance2));
    JobConfig jobConfig = prepareJobConfig();
    JobContext jobContext = prepareJobContext(TaskPartitionState.RUNNING, masterInstance);
    WorkflowConfig workflowConfig = prepareWorkflowConfig();
    WorkflowContext workflowContext = prepareWorkflowContext();
    Set<Integer> partitionSet = new HashSet<>(Collections.singletonList(PARTITION_ID));
    Map<String, IdealState> idealStates =
        prepareIdealStates(masterInstance, slaveInstance1, slaveInstance2);
    TaskAssignmentCalculator taskAssignmentCal =
        new FixedTargetTaskAssignmentCalculator(_assignableInstanceManager);
    Map<String, SortedSet<Integer>> result =
        taskAssignmentCal.getTaskAssignment(currentStateOutput, instances, jobConfig, jobContext,
            workflowConfig, workflowContext, partitionSet, idealStates);
    Assert.assertEquals(result.get(masterInstance).size(),0);
    Assert.assertEquals(result.get(slaveInstance1).size(),0);
    Assert.assertEquals(result.get(slaveInstance2).size(),0);
  }

  /**
   * Test FixedTargetTaskAssignmentCalculator and make sure that if a job has been assigned
   * before and target partition is still on the same instance and in INIT state,
   * we do not make new assignment for that task.
   */
  @Test
  public void testFixedTargetTaskAssignmentCalculatorSameInstanceInitTask() {
    _assignableInstanceManager = new AssignableInstanceManager();
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig,
        new TaskDataCache("CLUSTER_NAME"), _liveInstances, _instanceConfigs);
    // Preparing the inputs
    String masterInstance = INSTANCE_PREFIX + 1;
    String slaveInstance1 = INSTANCE_PREFIX + 2;
    String slaveInstance2 = INSTANCE_PREFIX + 3;
    // Preparing the inputs
    CurrentStateOutput currentStateOutput = prepareCurrentState(TaskPartitionState.INIT,
        masterInstance, slaveInstance1, slaveInstance2);
    List<String> instances =
        new ArrayList<String>(Arrays.asList(masterInstance, slaveInstance1, slaveInstance2));
    JobConfig jobConfig = prepareJobConfig();
    JobContext jobContext = prepareJobContext(TaskPartitionState.INIT, masterInstance);
    WorkflowConfig workflowConfig = prepareWorkflowConfig();
    WorkflowContext workflowContext = prepareWorkflowContext();
    Set<Integer> partitionSet = new HashSet<>(Collections.singletonList(PARTITION_ID));
    Map<String, IdealState> idealStates =
        prepareIdealStates(masterInstance, slaveInstance1, slaveInstance2);
    TaskAssignmentCalculator taskAssignmentCal =
        new FixedTargetTaskAssignmentCalculator(_assignableInstanceManager);
    Map<String, SortedSet<Integer>> result =
        taskAssignmentCal.getTaskAssignment(currentStateOutput, instances, jobConfig, jobContext,
            workflowConfig, workflowContext, partitionSet, idealStates);
    Assert.assertEquals(result.get(masterInstance).size(),0);
    Assert.assertEquals(result.get(slaveInstance1).size(),0);
    Assert.assertEquals(result.get(slaveInstance2).size(),0);
  }

  /**
   * Test FixedTargetTaskAssignmentCalculator and make sure that if a job has been assigned
   * before and target partition has moved to another instance, controller assign the task to
   * new/correct instance.
   */
  @Test
  public void testFixedTargetTaskAssignmentCalculatorDifferentInstance() {
    _assignableInstanceManager = new AssignableInstanceManager();
    _assignableInstanceManager.buildAssignableInstances(_clusterConfig,
        new TaskDataCache("CLUSTER_NAME"), _liveInstances, _instanceConfigs);
    // Preparing the inputs
    String masterInstance = INSTANCE_PREFIX + 2;
    String slaveInstance1 = INSTANCE_PREFIX + 1;
    String slaveInstance2 = INSTANCE_PREFIX + 3;
    // Preparing the inputs
    CurrentStateOutput currentStateOutput = prepareCurrentState(TaskPartitionState.RUNNING,
        masterInstance, slaveInstance1, slaveInstance2);
    List<String> instances =
        new ArrayList<String>(Arrays.asList(masterInstance, slaveInstance1, slaveInstance2));
    JobConfig jobConfig = prepareJobConfig();
    JobContext jobContext = prepareJobContext(TaskPartitionState.RUNNING, slaveInstance1);
    WorkflowConfig workflowConfig = prepareWorkflowConfig();
    WorkflowContext workflowContext = prepareWorkflowContext();
    Set<Integer> partitionSet = new HashSet<>(Collections.singletonList(PARTITION_ID));
    Map<String, IdealState> idealStates =
        prepareIdealStates(masterInstance, slaveInstance1, slaveInstance2);
    TaskAssignmentCalculator taskAssignmentCal =
        new FixedTargetTaskAssignmentCalculator(_assignableInstanceManager);
    Map<String, SortedSet<Integer>> result =
        taskAssignmentCal.getTaskAssignment(currentStateOutput, instances, jobConfig, jobContext,
            workflowConfig, workflowContext, partitionSet, idealStates);
    Assert.assertEquals(result.get(slaveInstance1).size(),0);
    Assert.assertEquals(result.get(masterInstance).size(),1);
    Assert.assertEquals(result.get(slaveInstance2).size(),0);
  }

  private JobConfig prepareJobConfig() {
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    jobConfigBuilder.setWorkflow(WORKFLOW_NAME);
    jobConfigBuilder.setCommand("TestCommand");
    jobConfigBuilder.setJobId(JOB_NAME);
    jobConfigBuilder.setTargetResource(TARGET_RESOURCES);
    List<String> targetPartition = new ArrayList<>();
    jobConfigBuilder.setTargetPartitions(targetPartition);
    Set<String> targetPartitionStates = new HashSet<>(Collections.singletonList("MASTER"));
    jobConfigBuilder.setTargetPartitions(targetPartition);
    jobConfigBuilder.setTargetPartitionStates(targetPartitionStates);
    List<TaskConfig> taskConfigs = new ArrayList<>();
    TaskConfig.Builder taskConfigBuilder = new TaskConfig.Builder();
    taskConfigBuilder.setTaskId("0");
    taskConfigs.add(taskConfigBuilder.build());
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    return jobConfigBuilder.build();
  }

  private JobContext prepareJobContext(TaskPartitionState taskPartitionState, String instance) {
    ZNRecord record = new ZNRecord(JOB_NAME);
    JobContext jobContext = new JobContext(record);
    jobContext.setStartTime(0L);
    jobContext.setName(JOB_NAME);
    jobContext.setStartTime(0L);
    jobContext.setPartitionState(PARTITION_ID, taskPartitionState);
    jobContext.setPartitionTarget(PARTITION_ID, TARGET_RESOURCES + "_" + TARGET_PARTITION_NAME);
    jobContext.setAssignedParticipant(PARTITION_ID, instance);
    return jobContext;
  }

  private CurrentStateOutput prepareCurrentState(TaskPartitionState taskCurrentState,
      String masterInstance, String slaveInstance1, String slaveInstance2) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    currentStateOutput.setResourceStateModelDef(JOB_NAME, "TASK");
    Partition taskPartition = new Partition(JOB_NAME + "_" + PARTITION_NAME);
    currentStateOutput.setCurrentState(JOB_NAME, taskPartition, masterInstance,
        taskCurrentState.name());
    Partition dbPartition = new Partition(TARGET_RESOURCES + "_0");
    currentStateOutput.setEndTime(TARGET_RESOURCES, dbPartition, masterInstance, 0L);
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, masterInstance, "MASTER");
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, slaveInstance1, "SLAVE");
    currentStateOutput.setCurrentState(TARGET_RESOURCES, dbPartition, slaveInstance2, "SLAVE");
    currentStateOutput.setInfo(TARGET_RESOURCES, dbPartition, masterInstance, "");
    return currentStateOutput;
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

  private Map<String, IdealState> prepareIdealStates(String instance1, String instance2,
      String instance3) {
    Map<String, IdealState> idealStates = new HashMap<>();

    ZNRecord recordDB = new ZNRecord(TARGET_RESOURCES);
    recordDB.setSimpleField(IdealState.IdealStateProperty.REPLICAS.name(), "3");
    recordDB.setSimpleField(IdealState.IdealStateProperty.REBALANCE_MODE.name(), "FULL_AUTO");
    recordDB.setSimpleField(IdealState.IdealStateProperty.IDEAL_STATE_MODE.name(),
        "AUTO_REBALANCE");
    recordDB.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(),
        "MasterSlave");
    recordDB.setSimpleField(IdealState.IdealStateProperty.STATE_MODEL_DEF_REF.name(),
        "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
    recordDB.setSimpleField(IdealState.IdealStateProperty.REBALANCER_CLASS_NAME.name(),
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
}
