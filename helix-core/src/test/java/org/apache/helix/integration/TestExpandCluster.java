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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Map;

import org.apache.helix.controller.strategy.TestEspressoStorageClusterIdealState;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.RebalanceUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestExpandCluster extends ZkStandAloneCMTestBase {
  @Test
  public void testExpandCluster() throws Exception {
    String DB2 = "TestDB2";
    int partitions = 100;
    int replica = 3;
    _setupTool.addResourceToCluster(CLUSTER_NAME, DB2, partitions, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, DB2, replica, "keyX");

    String DB3 = "TestDB3";

    _setupTool.addResourceToCluster(CLUSTER_NAME, DB3, partitions, STATE_MODEL);

    IdealState testDB0 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    IdealState testDB2 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB2);
    IdealState testDB3 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB3);

    for (int i = 0; i < 5; i++) {
      String storageNodeName = "localhost_" + (27960 + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    String command = "-zkSvr localhost:2183 -expandCluster " + CLUSTER_NAME;
    ClusterSetup.processCommandLineArgs(command.split(" "));

    IdealState testDB0_1 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    IdealState testDB2_1 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB2);
    IdealState testDB3_1 =
        _setupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, DB3);

    Map<String, Object> resultOld2 = RebalanceUtil.buildInternalIdealState(testDB2);
    Map<String, Object> result2 = RebalanceUtil.buildInternalIdealState(testDB2_1);

    TestEspressoStorageClusterIdealState.Verify(result2, partitions, replica - 1);

    Double masterKeepRatio = 0.0, slaveKeepRatio = 0.0;
    double[] result = TestEspressoStorageClusterIdealState.compareResult(resultOld2, result2);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(masterKeepRatio > 0.49 && masterKeepRatio < 0.51);

    Assert.assertTrue(testDB3_1.getRecord().getListFields().size() == 0);

    // partitions should stay as same
    Assert.assertTrue(testDB0_1.getRecord().getListFields().keySet()
        .containsAll(testDB0.getRecord().getListFields().keySet()));
    Assert.assertTrue(testDB0_1.getRecord().getListFields().size() == testDB0.getRecord()
        .getListFields().size());
    Assert.assertTrue(testDB2_1.getRecord().getMapFields().keySet()
        .containsAll(testDB2.getRecord().getMapFields().keySet()));
    Assert.assertTrue(testDB2_1.getRecord().getMapFields().size() == testDB2.getRecord()
        .getMapFields().size());
    Assert.assertTrue(testDB3_1.getRecord().getMapFields().keySet()
        .containsAll(testDB3.getRecord().getMapFields().keySet()));
    Assert.assertTrue(testDB3_1.getRecord().getMapFields().size() == testDB3.getRecord()
        .getMapFields().size());

    Map<String, Object> resultOld = RebalanceUtil.buildInternalIdealState(testDB0);
    Map<String, Object> resultNew = RebalanceUtil.buildInternalIdealState(testDB0_1);

    result = TestEspressoStorageClusterIdealState.compareResult(resultOld, resultNew);
    masterKeepRatio = result[0];
    slaveKeepRatio = result[1];
    Assert.assertTrue(masterKeepRatio > 0.49 && masterKeepRatio < 0.51);
  }
}
