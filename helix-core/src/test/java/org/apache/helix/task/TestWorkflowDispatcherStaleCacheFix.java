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

import java.lang.reflect.Field;
import java.util.Collections;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.monitoring.mbeans.ClusterStatusMonitor;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for WorkflowDispatcher to verify the stale cache fix.
 * Tests focus on verifying behavior differences between MessageChange and ResourceConfigChange events.
 */
public class TestWorkflowDispatcherStaleCacheFix {

  @Mock private HelixManager manager;
  @Mock private HelixDataAccessor accessor;
  @Mock private PropertyKey.Builder keyBuilder;
  @Mock private WorkflowControllerDataProvider clusterDataCache;
  @Mock private CurrentStateOutput currentStateOutput;
  @Mock private BestPossibleStateOutput bestPossibleOutput;
  @Mock private ClusterStatusMonitor clusterStatusMonitor;
  @Mock private TaskDataCache taskDataCache;
  @Mock private ZkHelixPropertyStore<ZNRecord> propertyStore;

  private WorkflowDispatcher dispatcher;
  private AutoCloseable mocks;

  @BeforeMethod
  public void setUp() throws Exception {
    mocks = MockitoAnnotations.openMocks(this);

    dispatcher = spy(new WorkflowDispatcher());
    dispatcher.init(manager);

    Field cacheField = WorkflowDispatcher.class.getDeclaredField("_clusterDataCache");
    cacheField.setAccessible(true);
    cacheField.set(dispatcher, clusterDataCache);

    Field monitorField = AbstractTaskDispatcher.class.getDeclaredField("_clusterStatusMonitor");
    monitorField.setAccessible(true);
    monitorField.set(dispatcher, clusterStatusMonitor);

    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(accessor.keyBuilder()).thenReturn(keyBuilder);
    when(manager.getClusterName()).thenReturn("TestCluster");
    when(clusterDataCache.getTaskDataCache()).thenReturn(taskDataCache);
    when(manager.getHelixPropertyStore()).thenReturn(propertyStore);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  /**
   * MessageChange event with stale DELETE cache triggers requireFullRefresh.
   * This is the primary bug scenario - MessageChange events don't refresh ResourceConfig cache.
   */
  @Test
  public void testMessageChangeEventWithStaleDelete() {
    String workflowName = "TestWorkflow";

    WorkflowConfig staleDeleteConfig = mock(WorkflowConfig.class);
    when(staleDeleteConfig.getTargetState()).thenReturn(TargetState.DELETE);
    when(staleDeleteConfig.getJobDag()).thenReturn(mock(JobDag.class));
    when(staleDeleteConfig.getJobDag().getAllNodes()).thenReturn(Collections.emptySet());

    WorkflowConfig freshStartConfig = mock(WorkflowConfig.class);
    when(freshStartConfig.getTargetState()).thenReturn(TargetState.START);
    doReturn(freshStartConfig).when(dispatcher).getFreshWorkflowConfig(workflowName);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(workflowContext.getFinishTime()).thenReturn(-1L);
    when(workflowContext.getJobStates()).thenReturn(Collections.emptyMap());

    dispatcher.updateWorkflowStatus(workflowName, staleDeleteConfig, workflowContext,
        currentStateOutput, bestPossibleOutput);

    // Verify: Fresh read was performed
    verify(dispatcher, times(1)).getFreshWorkflowConfig(workflowName);

    // Verify: requireFullRefresh was called due to stale DELETE detection
    verify(clusterDataCache, times(1)).requireFullRefresh();

    // Verify: Workflow was NOT deleted
    verify(accessor, never()).removeProperty(any(PropertyKey.class));
  }

  /**
   * ResourceConfigChange event with fresh cache doesn't need validation.
   * ResourceConfigChange events refresh the cache, so no stale DELETE issue.
   */
  @Test
  public void testResourceConfigChangeEventWithFreshCache() {
    String workflowName = "TestWorkflow";

    WorkflowConfig freshStartConfig = mock(WorkflowConfig.class);
    when(freshStartConfig.getTargetState()).thenReturn(TargetState.START);
    when(freshStartConfig.getJobDag()).thenReturn(mock(JobDag.class));
    when(freshStartConfig.getJobDag().getAllNodes()).thenReturn(Collections.emptySet());

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(workflowContext.getFinishTime()).thenReturn(-1L);
    when(workflowContext.getJobStates()).thenReturn(Collections.emptyMap());

    dispatcher.updateWorkflowStatus(workflowName, freshStartConfig, workflowContext,
        currentStateOutput, bestPossibleOutput);

    // Verify: No fresh read needed (state is not DELETE)
    verify(dispatcher, never()).getFreshWorkflowConfig(anyString());

    // Verify: No requireFullRefresh needed
    verify(clusterDataCache, never()).requireFullRefresh();

    // Verify: No deletion attempted
    verify(accessor, never()).removeProperty(any(PropertyKey.class));
  }

  /**
   * Legitimate DELETE still works when fresh state confirms DELETE.
   */
  @Test
  public void testLegitimateDeleteWithFreshValidation() {
    String workflowName = "TestWorkflow";

    WorkflowConfig deleteConfig = mock(WorkflowConfig.class);
    when(deleteConfig.getTargetState()).thenReturn(TargetState.DELETE);
    when(deleteConfig.isTerminable()).thenReturn(true);
    when(deleteConfig.getJobDag()).thenReturn(mock(JobDag.class));
    when(deleteConfig.getJobDag().getAllNodes()).thenReturn(Collections.emptySet());

    doReturn(deleteConfig).when(dispatcher).getFreshWorkflowConfig(workflowName);
    when(clusterDataCache.getWorkflowConfig(workflowName)).thenReturn(deleteConfig);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(workflowContext.getFinishTime()).thenReturn(System.currentTimeMillis());
    when(workflowContext.getJobStates()).thenReturn(Collections.emptyMap());

    PropertyKey workflowKey = mock(PropertyKey.class);
    when(keyBuilder.resourceConfig(workflowName)).thenReturn(workflowKey);
    when(keyBuilder.idealStates(workflowName)).thenReturn(mock(PropertyKey.class));
    when(keyBuilder.workflowContext(workflowName)).thenReturn(mock(PropertyKey.class));
    when(accessor.removeProperty(any(PropertyKey.class))).thenReturn(true);
    when(accessor.getPropertyStat(any(PropertyKey.class)))
        .thenReturn(mock(org.apache.helix.HelixProperty.Stat.class));

    dispatcher.updateWorkflowStatus(workflowName, deleteConfig, workflowContext,
        currentStateOutput, bestPossibleOutput);

    // Verify: Fresh read was performed
    verify(dispatcher, times(1)).getFreshWorkflowConfig(workflowName);

    // Verify: Deletion proceeded (both states confirmed DELETE)
    verify(accessor, atLeastOnce()).removeProperty(any(PropertyKey.class));

    // Verify: No unnecessary full refresh
    verify(clusterDataCache, never()).requireFullRefresh();
  }

  /**
   * Workflow already deleted from ZK (fresh config null).
   */
  @Test
  public void testWorkflowAlreadyDeletedFromZK() {
    String workflowName = "TestWorkflow";

    WorkflowConfig deleteConfig = mock(WorkflowConfig.class);
    when(deleteConfig.getTargetState()).thenReturn(TargetState.DELETE);
    when(deleteConfig.getJobDag()).thenReturn(mock(JobDag.class));
    when(deleteConfig.getJobDag().getAllNodes()).thenReturn(Collections.emptySet());

    doReturn(null).when(dispatcher).getFreshWorkflowConfig(workflowName);

    WorkflowContext workflowContext = mock(WorkflowContext.class);
    when(workflowContext.getFinishTime()).thenReturn(System.currentTimeMillis());
    when(workflowContext.getJobStates()).thenReturn(Collections.emptyMap());

    dispatcher.updateWorkflowStatus(workflowName, deleteConfig, workflowContext,
        currentStateOutput, bestPossibleOutput);

    // Verify: Fresh read was performed
    verify(dispatcher, times(1)).getFreshWorkflowConfig(workflowName);

    // Verify: No deletion attempted (already gone)
    verify(accessor, never()).removeProperty(any(PropertyKey.class));

    // Verify: No full refresh needed
    verify(clusterDataCache, never()).requireFullRefresh();
  }
}