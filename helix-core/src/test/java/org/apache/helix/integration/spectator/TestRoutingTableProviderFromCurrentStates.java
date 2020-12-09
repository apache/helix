package org.apache.helix.integration.spectator;

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

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.SystemPropertyKeys;
import org.apache.helix.TestHelper;
import org.apache.helix.api.listeners.PreFetch;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.MockTask;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.apache.helix.monitoring.mbeans.MonitorDomainNames;
import org.apache.helix.monitoring.mbeans.RoutingTableProviderMonitor;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.task.Workflow;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProviderFromCurrentStates extends ZkTestBase {
  private HelixManager _manager;
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private MBeanServer _beanServer = ManagementFactory.getPlatformMBeanServer();

  @BeforeClass
  public void beforeClass() throws Exception {
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    Map<String, TaskFactory> taskFactoryReg = new HashMap<>();
    taskFactoryReg.put(MockTask.TASK_COMMAND, MockTask::new);

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      stateMachine.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
          new TaskStateModelFactory(_participants[i], taskFactoryReg));
      _participants[i].syncStart();
    }

    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    ConfigAccessor _configAccessor = _manager.getConfigAccessor();
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.enableTargetExternalView(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /*
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testCurrentStatesRoutingTableIgnoreTaskCurrentStates() throws Exception {
    FlaggedCurrentStateRoutingTableProvider routingTableCurrentStates =
        new FlaggedCurrentStateRoutingTableProvider(_manager);
    Assert.assertFalse(routingTableCurrentStates.isOnStateChangeTriggered());

    try {
      TaskDriver taskDriver = new TaskDriver(_manager);
      String workflowName1 = TestHelper.getTestMethodName() + "_1";
      String jobName = "JOB0";

      JobConfig.Builder jobBuilder =
          new JobConfig.Builder().setWorkflow(workflowName1).setNumberOfTasks(NUM_NODES)
              .setNumConcurrentTasksPerInstance(1).setCommand(MockTask.TASK_COMMAND)
              .setJobCommandConfigMap(ImmutableMap.of(MockTask.JOB_DELAY, "1000"));

      Workflow.Builder workflowBuilder1 =
          new Workflow.Builder(workflowName1).addJob(jobName, jobBuilder);
      taskDriver.start(workflowBuilder1.build());
      taskDriver
          .pollForJobState(workflowName1, TaskUtil.getNamespacedJobName(workflowName1, jobName),
              TaskState.COMPLETED);

      Assert.assertFalse(routingTableCurrentStates.isOnStateChangeTriggered());

      // Disable the task current path and the routing table provider should be notified
      System.setProperty(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED, "true");
      String workflowName2 = TestHelper.getTestMethodName() + "_2";
      Workflow.Builder workflowBuilder2 =
          new Workflow.Builder(workflowName2).addJob(jobName, jobBuilder);
      taskDriver.start(workflowBuilder2.build());
      taskDriver
          .pollForJobState(workflowName2, TaskUtil.getNamespacedJobName(workflowName2, jobName),
              TaskState.COMPLETED);

      Assert.assertTrue(routingTableCurrentStates.isOnStateChangeTriggered());
      System.setProperty(SystemPropertyKeys.TASK_CURRENT_STATE_PATH_DISABLED, "false");

      String dbName = "testDB";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, dbName, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, NUM_REPLICAS);

      ZkHelixClusterVerifier clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
              .build();
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      Assert.assertTrue(routingTableCurrentStates.isOnStateChangeTriggered());
      _gSetupTool.dropResourceFromCluster(CLUSTER_NAME, dbName);
    } finally {
      routingTableCurrentStates.shutdown();
    }
  }

  @Test (dependsOnMethods = "testCurrentStatesRoutingTableIgnoreTaskCurrentStates")
  public void testRoutingTableWithCurrentStates() throws Exception {
    RoutingTableProvider routingTableEV =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    RoutingTableProvider routingTableCurrentStates =
        new RoutingTableProvider(_manager, PropertyType.CURRENTSTATES);

    try {
      String db1 = "TestDB-1";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db1, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      long startTime = System.currentTimeMillis();
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db1, NUM_REPLICAS);

      ZkHelixClusterVerifier clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
              .build();
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      IdealState idealState1 =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db1);
      validate(idealState1, routingTableEV, routingTableCurrentStates, 0);

      // add new DB
      String db2 = "TestDB-2";
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db2, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      startTime = System.currentTimeMillis();
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db2, NUM_REPLICAS);

      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      IdealState idealState2 =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db2);
      validate(idealState2, routingTableEV, routingTableCurrentStates, 0);

      // shutdown an instance
      startTime = System.currentTimeMillis();
      _participants[0].syncStop();
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      validatePropagationLatency(PropertyType.CURRENTSTATES,
          System.currentTimeMillis() - startTime);

      validate(idealState1, routingTableEV, routingTableCurrentStates, 0);
      validate(idealState2, routingTableEV, routingTableCurrentStates, 0);
    } finally {
      routingTableEV.shutdown();
      routingTableCurrentStates.shutdown();
    }
  }

  private ObjectName buildObjectName(PropertyType type) throws MalformedObjectNameException {
    return MBeanRegistrar.buildObjectName(MonitorDomainNames.RoutingTableProvider.name(),
        RoutingTableProviderMonitor.CLUSTER_KEY, CLUSTER_NAME,
        RoutingTableProviderMonitor.DATA_TYPE_KEY, type.name());
  }

  private void validatePropagationLatency(PropertyType type, final long upperBound)
      throws Exception {
    final ObjectName name = buildObjectName(type);
    Assert.assertTrue(TestHelper.verify(() -> {
      long stateLatency = (long) _beanServer.getAttribute(name, "StatePropagationLatencyGauge.Max");
      return stateLatency > 0 && stateLatency <= upperBound;
    }, 1000));
  }

  @Test(dependsOnMethods = "testRoutingTableWithCurrentStates")
  public void TestInconsistentStateEventProcessing() throws Exception {
    // This test requires an additional HelixManager since one of the provider event processing will
    // be blocked.
    HelixManager helixManager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, TestHelper.getTestMethodName(), InstanceType.SPECTATOR,
            ZK_ADDR);
    helixManager.connect();
    RoutingTableProvider routingTableEV = null;
    BlockingCurrentStateRoutingTableProvider routingTableCS = null;
    int shutdownParticipantIndex = -1;

    try {
      // Prepare test environment
      routingTableEV = new RoutingTableProvider(helixManager, PropertyType.EXTERNALVIEW);
      routingTableCS = new BlockingCurrentStateRoutingTableProvider(_manager);
      // Ensure the current state routing table provider has been refreshed.
      for (String resourceName : _gSetupTool.getClusterManagementTool()
          .getResourcesInCluster(CLUSTER_NAME)) {
        IdealState idealState = _gSetupTool.getClusterManagementTool()
            .getResourceIdealState(CLUSTER_NAME, resourceName);
        validate(idealState, routingTableEV, routingTableCS, 3000);
      }

      // Blocking the current state event processing
      routingTableCS.setBlocking(true);
      // 1. Create a new resource and wait until all components process the change.
      // Note that since the processing of routingTableCS has been blocked, it will be stuck on the
      // current state change.
      String db = "TestDB-" + TestHelper.getTestMethodName();
      _gSetupTool.addResourceToCluster(CLUSTER_NAME, db, NUM_PARTITIONS, "MasterSlave",
          IdealState.RebalanceMode.FULL_AUTO.name(), CrushEdRebalanceStrategy.class.getName());
      _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, db, NUM_REPLICAS);
      ZkHelixClusterVerifier clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
              .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
              .build();
      Assert.assertTrue(clusterVerifier.verifyByPolling(5000, 500));
      // 2. Process one event, so the current state will be refreshed with the new DB partitions
      routingTableCS.proceedNewEventHandling();
      IdealState idealState =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, db);
      String targetPartitionName = idealState.getPartitionSet().iterator().next();
      // Wait until the routingtable is updated.
      BlockingCurrentStateRoutingTableProvider finalRoutingTableCS = routingTableCS;
      Assert.assertTrue(TestHelper.verify(
          () -> finalRoutingTableCS.getInstances(db, targetPartitionName, "MASTER").size() > 0,
          2000));
      String targetNodeName =
          routingTableCS.getInstances(db, targetPartitionName, "MASTER").get(0).getInstanceName();
      // 3. Shutdown one of the instance that contains a master partition
      for (int i = 0; i < _participants.length; i++) {
        if (_participants[i].getInstanceName().equals(targetNodeName)) {
          shutdownParticipantIndex = i;
          _participants[i].syncStop();
        }
      }
      Assert.assertTrue(clusterVerifier.verifyByPolling());
      // 4. Process one of the stale current state event.
      // The expectation is that, the provider will refresh with all the latest data including the
      // the live instance change. Even the corresponding ZK event has not been processed yet.
      routingTableCS.proceedNewEventHandling();
      validate(idealState, routingTableEV, routingTableCS, 3000);
      // 5. Unblock the event processing and let the provider to process all events.
      // The expectation is that, the eventual routing tables are still the same.
      routingTableCS.setBlocking(false);
      routingTableCS.proceedNewEventHandling();
      // Wait for a short while so the router will process at least one event.
      Thread.sleep(500);
      // Confirm that 2 providers match eventually
      validate(idealState, routingTableEV, routingTableCS, 2000);
    } finally {
      if (routingTableCS != null) {
        routingTableCS.setBlocking(false);
        routingTableCS.proceedNewEventHandling();
        routingTableCS.shutdown();
      }
      if (routingTableEV != null) {
        routingTableEV.shutdown();
      }
      if (shutdownParticipantIndex >= 0) {
        String participantName = _participants[shutdownParticipantIndex].getInstanceName();
        _participants[shutdownParticipantIndex] =
            new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, participantName);
        _participants[shutdownParticipantIndex].syncStart();
      }
      helixManager.disconnect();
    }
  }

  @Test(dependsOnMethods = { "TestInconsistentStateEventProcessing" })
  public void testWithSupportSourceDataType() {
    new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW).shutdown();
    new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW).shutdown();
    new RoutingTableProvider(_manager, PropertyType.CURRENTSTATES).shutdown();

    try {
      new RoutingTableProvider(_manager, PropertyType.IDEALSTATES).shutdown();
      Assert.fail();
    } catch (HelixException ex) {
      Assert.assertTrue(ex.getMessage().contains("Unsupported source data type"));
    }
  }

  private void validate(IdealState idealState, RoutingTableProvider routingTableEV,
      RoutingTableProvider routingTableCurrentStates, int timeout) throws Exception {
    Assert.assertTrue(TestHelper
        .verify(() -> compare(idealState, routingTableEV, routingTableCurrentStates), timeout));
  }

  private boolean compare(IdealState idealState, RoutingTableProvider routingTableEV,
      RoutingTableProvider routingTableCurrentStates) {
    String db = idealState.getResourceName();
    Set<String> partitions = idealState.getPartitionSet();
    for (String partition : partitions) {
      List<InstanceConfig> masterInsEv =
          routingTableEV.getInstancesForResource(db, partition, "MASTER");
      List<InstanceConfig> masterInsCs =
          routingTableCurrentStates.getInstancesForResource(db, partition, "MASTER");
      if (masterInsEv.size() != 1 || masterInsCs.size() != 1 || !masterInsCs.equals(masterInsEv)) {
        return false;
      }
      List<InstanceConfig> slaveInsEv =
          routingTableEV.getInstancesForResource(db, partition, "SLAVE");
      List<InstanceConfig> slaveInsCs =
          routingTableCurrentStates.getInstancesForResource(db, partition, "SLAVE");
      if (slaveInsEv.size() != 2 || slaveInsCs.size() != 2 || !new HashSet(slaveInsCs)
          .equals(new HashSet(slaveInsEv))) {
        return false;
      }
    }
    return true;
  }

  static class BlockingCurrentStateRoutingTableProvider extends RoutingTableProvider {
    private final Semaphore newEventHandlingCount = new Semaphore(0);
    private boolean _isBlocking = false;

    public BlockingCurrentStateRoutingTableProvider(HelixManager manager) {
      super(manager, PropertyType.CURRENTSTATES);
    }

    public void setBlocking(boolean isBlocking) {
      _isBlocking = isBlocking;
    }

    void proceedNewEventHandling() {
      newEventHandlingCount.release();
    }

    @Override
    @PreFetch(enabled = false)
    public void onStateChange(String instanceName, List<CurrentState> statesInfo,
        NotificationContext changeContext) {
      if (_isBlocking) {
        try {
          newEventHandlingCount.acquire();
        } catch (InterruptedException e) {
          throw new HelixException("Failed to acquire handling lock for testing.");
        }
      }
      super.onStateChange(instanceName, statesInfo, changeContext);
    }

    @Override
    @PreFetch(enabled = true)
    public void onLiveInstanceChange(List<LiveInstance> liveInstances,
        NotificationContext changeContext) {
      if (_isBlocking) {
        try {
          newEventHandlingCount.acquire();
        } catch (InterruptedException e) {
          throw new HelixException("Failed to acquire handling lock for testing.");
        }
      }
      super.onLiveInstanceChange(liveInstances, changeContext);
    }
  }

  static class FlaggedCurrentStateRoutingTableProvider extends RoutingTableProvider {
    private boolean onStateChangeTriggered = false;

    public FlaggedCurrentStateRoutingTableProvider(HelixManager manager) {
      super(manager, PropertyType.CURRENTSTATES);
    }

    public boolean isOnStateChangeTriggered() {
      return onStateChangeTriggered;
    }

    @Override
    @PreFetch(enabled = false)
    public void onStateChange(String instanceName, List<CurrentState> statesInfo,
        NotificationContext changeContext) {
      onStateChangeTriggered = true;
      super.onStateChange(instanceName, statesInfo, changeContext);
    }
  }
}
