package org.apache.helix.controller.stages;

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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.DedupEventProcessor;
import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;
import org.apache.helix.controller.pipeline.AsyncWorkerType;
import org.apache.helix.task.TaskUtil;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.caches.TaskDataCache;
import org.apache.helix.controller.dataproviders.WorkflowControllerDataProvider;
import org.apache.helix.controller.stages.task.TaskPersistDataStage;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.TaskTestUtil;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.JobContext;
import org.apache.helix.task.JobQueue;
import org.apache.helix.task.TaskPartitionState;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.WorkflowContext;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestTaskStage extends TaskTestBase {
  private ClusterEvent _event =
      new ClusterEvent(CLUSTER_NAME, ClusterEventType.TaskCurrentStateChange);
  private PropertyKey.Builder _keyBuilder;
  private String _testWorkflow = TestHelper.getTestClassName();
  private String _testJobPrefix = _testWorkflow + "_Job_";

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();
    // Stop the controller for isolated testing of the stage
    _controller.syncStop();
    _keyBuilder = _manager.getHelixDataAccessor().keyBuilder();
  }

  @Test
  public void testPersistContextData() {
    _event.addAttribute(AttributeName.helixmanager.name(), _manager);

    WorkflowControllerDataProvider cache = new WorkflowControllerDataProvider(CLUSTER_NAME);
    TaskDataCache taskDataCache = cache.getTaskDataCache();

    // Build a queue
    JobQueue.Builder queueBuilder = TaskTestUtil.buildJobQueue(_testWorkflow);
    JobConfig.Builder jobBuilder_0 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    JobConfig.Builder jobBuilder_1 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    JobConfig.Builder jobBuilder_2 =
        new JobConfig.Builder().setJobId("Job_0").setTargetResource("1").setCommand("1");
    queueBuilder.enqueueJob("Job_0", jobBuilder_0).enqueueJob("Job_1", jobBuilder_1)
        .enqueueJob("Job_2", jobBuilder_2);

    _driver.createQueue(queueBuilder.build());
    // Manually trigger a cache refresh
    cache.refresh(new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor));

    // Create the context
    WorkflowContext wfCtx = new WorkflowContext(new ZNRecord(TaskUtil.WORKFLOW_CONTEXT_KW));
    wfCtx.setJobState(_testJobPrefix + "0", TaskState.COMPLETED);
    wfCtx.setJobState(_testJobPrefix + "1", TaskState.COMPLETED);
    wfCtx.setWorkflowState(TaskState.IN_PROGRESS);
    wfCtx.setName(_testWorkflow);
    wfCtx.setStartTime(System.currentTimeMillis());

    JobContext jbCtx0 = new JobContext(new ZNRecord(_testJobPrefix + "0"));
    jbCtx0.setName(_testJobPrefix + "0");
    jbCtx0.setStartTime(System.currentTimeMillis());
    jbCtx0.setPartitionState(0, TaskPartitionState.COMPLETED);

    JobContext jbCtx1 = new JobContext((new ZNRecord(_testJobPrefix + "1")));
    jbCtx1.setName(_testJobPrefix + "1");
    jbCtx1.setStartTime(System.currentTimeMillis());
    jbCtx1.setPartitionState(0, TaskPartitionState.COMPLETED);

    taskDataCache.updateWorkflowContext(_testWorkflow, wfCtx);
    taskDataCache.updateJobContext(_testJobPrefix + "0", jbCtx0);
    taskDataCache.updateJobContext(_testJobPrefix + "1", jbCtx1);

    _event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);

    // Write contexts to ZK first
    TaskPersistDataStage persistDataStage = new TaskPersistDataStage();
    persistDataStage.process(_event);

    Assert.assertNotNull(_driver.getWorkflowContext(_testWorkflow));
    Assert.assertNotNull(_driver.getJobContext(_testJobPrefix + "0"));
    Assert.assertNotNull(_driver.getJobContext(_testJobPrefix + "1"));

    jbCtx0.setPartitionState(0, TaskPartitionState.ERROR);
    wfCtx.setJobState(_testJobPrefix + "0", TaskState.FAILED);
    taskDataCache.updateJobContext(_testJobPrefix + "0", jbCtx0);

    wfCtx.getJobStates().remove(_testJobPrefix + "1");
    taskDataCache.removeContext(_testJobPrefix + "1");

    JobContext jbCtx2 = new JobContext(new ZNRecord(_testJobPrefix + "2"));
    jbCtx2.setName(_testJobPrefix + "2");
    jbCtx2.setPartitionState(1, TaskPartitionState.INIT);
    wfCtx.setJobState(_testJobPrefix + "2", TaskState.IN_PROGRESS);
    taskDataCache.updateJobContext(_testJobPrefix + "2", jbCtx2);

    taskDataCache.updateWorkflowContext(_testWorkflow, wfCtx);

    persistDataStage.process(_event);

    Assert.assertEquals(_driver.getWorkflowContext(_testWorkflow), wfCtx);
    Assert.assertEquals(_driver.getJobContext(_testJobPrefix + "0"), jbCtx0);
    Assert.assertEquals(_driver.getJobContext(_testJobPrefix + "2"), jbCtx2);
    Assert.assertNull(_driver.getJobContext(_testJobPrefix + "1"));
  }

  /**
   * Test that if there is a job in the DAG with JobConfig gone (due to ZK delete failure), the
   * async job purge will try to delete it again.
   */
  @Test(dependsOnMethods = "testPersistContextData")
  public void testPartialDataPurge() throws Exception {
    DedupEventProcessor<String, Runnable> worker =
        new DedupEventProcessor<String, Runnable>(CLUSTER_NAME,
            AsyncWorkerType.TaskJobPurgeWorker.name()) {
          @Override
          protected void handleEvent(Runnable event) {
            event.run();
          }
        };
    worker.start();
    Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> workerPool = new HashMap<>();
    workerPool.put(AsyncWorkerType.TaskJobPurgeWorker, worker);
    _event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), workerPool);

    // Manually delete JobConfig
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "0");
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "1");
    deleteJobConfigs(_testWorkflow, _testJobPrefix + "2");

    // Manually refresh because there's no controller notify data change
    BaseControllerDataProvider dataProvider =
        _event.getAttribute(AttributeName.ControllerDataProvider.name());
    dataProvider.notifyDataChange(HelixConstants.ChangeType.RESOURCE_CONFIG);
    dataProvider.refresh(_manager.getHelixDataAccessor());

    // Then purge jobs
    TaskGarbageCollectionStage garbageCollectionStage = new TaskGarbageCollectionStage();
    garbageCollectionStage.process(_event);

    // Check that contexts have been purged for the 2 jobs in both old and new ZNode paths
    checkForContextRemoval(_testWorkflow, _testJobPrefix + "0");
    checkForContextRemoval(_testWorkflow, _testJobPrefix + "1");
    checkForContextRemoval(_testWorkflow, _testJobPrefix + "2");
  }

  @Test(dependsOnMethods = "testPartialDataPurge")
  public void testWorkflowGarbageCollection() throws Exception {
    DedupEventProcessor<String, Runnable> worker =
        new DedupEventProcessor<String, Runnable>(CLUSTER_NAME,
            AsyncWorkerType.TaskJobPurgeWorker.name()) {
          @Override
          protected void handleEvent(Runnable event) {
            event.run();
          }
        };
    worker.start();
    Map<AsyncWorkerType, DedupEventProcessor<String, Runnable>> workerPool = new HashMap<>();
    workerPool.put(AsyncWorkerType.TaskJobPurgeWorker, worker);
    _event.addAttribute(AttributeName.AsyncFIFOWorkerPool.name(), workerPool);

    String zkPath =
        _manager.getHelixDataAccessor().keyBuilder().resourceConfig(_testWorkflow).getPath();
    _baseAccessor.remove(zkPath, AccessOption.PERSISTENT);

    // Manually refresh because there's no controller notify data change
    BaseControllerDataProvider dataProvider =
        _event.getAttribute(AttributeName.ControllerDataProvider.name());
    dataProvider.notifyDataChange(HelixConstants.ChangeType.RESOURCE_CONFIG);
    dataProvider.refresh(_manager.getHelixDataAccessor());

    // Then garbage collect workflow
    TaskGarbageCollectionStage garbageCollectionStage = new TaskGarbageCollectionStage();
    garbageCollectionStage.process(_event);

    // Check that contexts have been purged for the workflow
    checkForContextRemoval(_testWorkflow);

    worker.shutdown();
  }

  private void deleteJobConfigs(String workflowName, String jobName) {
    String oldPath = _manager.getHelixDataAccessor().keyBuilder().resourceConfig(jobName).getPath();
    String newPath = _manager.getHelixDataAccessor().keyBuilder()
        .jobConfigZNode(workflowName, jobName).getPath();
    _baseAccessor.remove(oldPath, AccessOption.PERSISTENT);
    _baseAccessor.remove(newPath, AccessOption.PERSISTENT);
  }

  private void checkForContextRemoval(String workflow, String job) throws Exception {
    // JobContexts in old ZNode path
    String oldPath =
        String.format("/%s/PROPERTYSTORE/TaskRebalancer/%s/Context", CLUSTER_NAME, job);
    String newPath = _keyBuilder.jobContextZNode(workflow, job).getPath();

    Assert.assertTrue(TestHelper.verify(
        () -> !_baseAccessor.exists(oldPath, AccessOption.PERSISTENT) && !_baseAccessor
            .exists(newPath, AccessOption.PERSISTENT), 120000));
  }

  private void checkForContextRemoval(String workflow) throws Exception {
    Assert.assertTrue(TestHelper.verify(() -> !_baseAccessor
            .exists(_keyBuilder.workflowContextZNode(workflow).getPath(), AccessOption.PERSISTENT),
        120000));
  }
}
