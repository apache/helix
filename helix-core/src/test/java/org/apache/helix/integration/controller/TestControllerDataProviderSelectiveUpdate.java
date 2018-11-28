package org.apache.helix.integration.controller;

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

import org.apache.helix.HelixConstants;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.mock.MockZkHelixDataAccessor;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestControllerDataProviderSelectiveUpdate extends ZkStandAloneCMTestBase {

  @Test()
  public void testUpdateOnNotification() throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    ResourceControllerDataProvider cache =
        new ResourceControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());
    cache.refresh(accessor);

    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), NODE_NR + 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    // cluster config always get reloaded
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    accessor.clearReadCounters();
    // refresh again should read nothing as ideal state is same
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    accessor.clearReadCounters();
    cache.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);
  }

  @Test(dependsOnMethods = {"testUpdateOnNotification"})
  public void testSelectiveUpdates() throws Exception {
    MockZkHelixDataAccessor accessor =
        new MockZkHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));

    ResourceControllerDataProvider cache =
        new ResourceControllerDataProvider("CLUSTER_" + TestHelper.getTestClassName());
    cache.refresh(accessor);

    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), NODE_NR + 1);

    accessor.clearReadCounters();

    // refresh again should read nothing
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.LIVEINSTANCES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), 0);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 1);

    // add a new resource
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_1", _PARTITIONS, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_1", _replica);

    Thread.sleep(100);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    accessor.clearReadCounters();

    // refresh again should read only new current states and new idealstate
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.CURRENTSTATES), NODE_NR);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 1);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);

    // Add more resources
    accessor.clearReadCounters();

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_2", _PARTITIONS, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_2", _replica);
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, "TestDB_3", _PARTITIONS, STATE_MODEL);
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, "TestDB_3", _replica);

    // Totally four resources. Two of them are newly added.
    cache.notifyDataChange(HelixConstants.ChangeType.IDEAL_STATE);
    cache.refresh(accessor);
    Assert.assertEquals(accessor.getReadCount(PropertyType.IDEALSTATES), 2);
    Assert.assertEquals(accessor.getReadCount(PropertyType.EXTERNALVIEW), 0);

    // Test WorkflowConfig/JobConfigs
    TaskDriver driver = new TaskDriver(_manager);
    Workflow.Builder workflow = WorkflowGenerator.generateSingleJobWorkflowBuilder("Job",
        new JobConfig.Builder().setCommand("ReIndex").setTargetResource("TestDB_2"));
    driver.start(workflow.build());

    Thread.sleep(100);
    accessor.clearReadCounters();

    cache.notifyDataChange(HelixConstants.ChangeType.RESOURCE_CONFIG);
    cache.refresh(accessor);
    // 1 Cluster Config change + 2 Resource Config Changes
    Assert.assertEquals(accessor.getReadCount(PropertyType.CONFIGS), 3);
  }
}
