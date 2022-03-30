package org.apache.helix.integration;

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

import java.util.Arrays;
import java.util.Map;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.constants.InstanceConstants;
import org.apache.helix.integration.task.TaskTestBase;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.util.InstanceValidationUtil;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestBatchEnableInstances extends TaskTestBase {
  private ConfigAccessor _accessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _numDbs = 1;
    _numReplicas = 3;
    _numNodes = 5;
    _numPartitions = 4;
    super.beforeClass();
    _accessor = new ConfigAccessor(_gZkClient);
  }

  @Test
  public void testOldEnableDisable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        _participants[0].getInstanceName(), false);
    Assert.assertTrue(_clusterVerifier.verify());

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numPartitions);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(!stateMap.keySet().contains(_participants[0].getInstanceName()));
    }
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        _participants[0].getInstanceName(), true);
  }

  @Test
  public void testBatchEnableDisable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numPartitions);
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      Assert.assertTrue(!stateMap.keySet().contains(_participants[0].getInstanceName()));
      Assert.assertTrue(!stateMap.keySet().contains(_participants[1].getInstanceName()));
    }
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
  }

  @Test
  public void testOldDisableBatchEnable() throws InterruptedException {
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        _participants[0].getInstanceName(), false);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numPartitions);
    int numOfFirstHost = 0;
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.keySet().contains(_participants[0].getInstanceName())) {
        numOfFirstHost++;
      }
    }
    Assert.assertTrue(numOfFirstHost > 0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        _participants[0].getInstanceName(), true);
  }

  @Test
  public void testBatchDisableOldEnable() throws InterruptedException {
    // disable 2 instances
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        false, InstanceConstants.InstanceDisabledType.USER_OPERATION, "reason_1");
    // check disabled type from InstanceValidationUtil
    HelixDataAccessor dataAccessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    Assert.assertEquals(InstanceValidationUtil
            .getInstanceHelixDisabledType(dataAccessor, _participants[1].getInstanceName()),
        InstanceConstants.InstanceDisabledType.USER_OPERATION.toString());
    // check disabled reason from getter in clusterConfig
    ClusterConfig clusterConfig =
        dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    Assert.assertEquals(
        clusterConfig.getInstanceHelixDisabledReason(_participants[1].getInstanceName()),
        "reason_1");
    Assert.assertNotNull(
        clusterConfig.getInstanceHelixDisabledTimeStamp(_participants[0].getInstanceName()));
    // enable the second instance
    _gSetupTool.getClusterManagementTool()
        .enableInstance(CLUSTER_NAME, _participants[0].getInstanceName(), true);
    // check disabled type for second instance from InstanceValidationUtil and clusterConfig
    Assert.assertEquals(InstanceValidationUtil
            .getInstanceHelixDisabledType(dataAccessor, _participants[0].getInstanceName()),
        InstanceConstants.InstanceDisabledType.INSTANCE_NOT_DISABLED.toString());
    clusterConfig = dataAccessor.getProperty(dataAccessor.keyBuilder().clusterConfig());
    Assert.assertEquals(
        clusterConfig.getInstanceHelixDisabledType(_participants[0].getInstanceName()),
        InstanceConstants.InstanceDisabledType.INSTANCE_NOT_DISABLED.toString());
    Assert.assertNull(
        clusterConfig.getInstanceHelixDisabledTimeStamp(_participants[0].getInstanceName()));

    Thread.sleep(2000);

    ExternalView externalView = _gSetupTool.getClusterManagementTool()
        .getResourceExternalView(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB);
    Assert.assertEquals(externalView.getRecord().getMapFields().size(), _numPartitions);
    int numOfFirstHost = 0;
    for (Map<String, String> stateMap : externalView.getRecord().getMapFields().values()) {
      if (stateMap.keySet().contains(_participants[0].getInstanceName())) {
        numOfFirstHost++;
      }
      Assert.assertTrue(!stateMap.keySet().contains(_participants[1].getInstanceName()));
    }
    Assert.assertTrue(numOfFirstHost > 0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
        Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
        true);
  }

  @Test
  public void testBatchDisableNegativeInput() {
    try {
      _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME,
          Arrays.asList(_participants[0].getInstanceName(), _participants[1].getInstanceName()),
          false, InstanceConstants.InstanceDisabledType.INSTANCE_NOT_DISABLED, "reason_1");
    } catch (HelixException ex) {
      Assert.assertEquals(ex.getMessage(),
          "Can not set INSTANCE_NOT_DISABLED as disabled type to an instance");
    }
  }
}
