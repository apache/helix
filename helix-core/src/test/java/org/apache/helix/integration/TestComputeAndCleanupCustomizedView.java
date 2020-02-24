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

import java.util.ArrayList;
import java.util.Date;

import java.util.List;
import java.util.Map;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateAggregationConfig;
import org.apache.helix.model.CustomizedView;
import org.testng.Assert;
import org.testng.annotations.Test;

import static java.lang.Thread.*;


/**
 * Test compute and clean customized view - if customized state is remove externally, controller should remove the
 * orphan customized view
 */
public class TestComputeAndCleanupCustomizedView extends ZkUnitTestBase {

  private final String RESOURCE_NAME = "TestDB0";
  private final String PARTITION_NAME = "TestDB0_0";
  private final String CUSTOMIZED_STATE_NAME = "customizedState1";
  private final String INSTANCE_NAME1 = "localhost_12918";
  private final String INSTANCE_NAME2 = "localhost_12919";

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    CustomizedStateAggregationConfig config = new CustomizedStateAggregationConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);

    accessor.setProperty(keyBuilder.customizedStateAggregationConfig(), config);

    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME1, CUSTOMIZED_STATE_NAME, RESOURCE_NAME),
        customizedState);

    CustomizedView customizedView = null;
    int i = 0;
    while (true)  {
      sleep(10000);
      try {
        customizedView = accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME, RESOURCE_NAME));
        Map<String, String> stateMap = customizedView.getRecord().getMapField(PARTITION_NAME);
        if (stateMap.get(INSTANCE_NAME1).equals("STARTED")) {
          System.out.println("succeed");
          break;
        }
      } catch (Exception e) {
          i ++;
          if (i >= 10) {
            Assert.fail("The customized view is not correct");
          }
      }
    }

    // disable controller
    ZKHelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableCluster(clusterName, false);
    ZkTestHelper.tryWaitZkEventsCleaned(controller.getZkClient());

    // drop resource
    admin.dropResource(clusterName, RESOURCE_NAME);

    // delete customized state manually, controller shall remove customized view when cluster is enabled again

    accessor.removeProperty(
        keyBuilder.customizedState(INSTANCE_NAME1, CUSTOMIZED_STATE_NAME, RESOURCE_NAME));
    accessor.removeProperty(
        keyBuilder.currentState(INSTANCE_NAME2, CUSTOMIZED_STATE_NAME, RESOURCE_NAME));

    // re-enable controller shall remove orphan external view
    // System.out.println("re-enabling controller");
    admin.enableCluster(clusterName, true);

    customizedView = null;
    for (i = 0; i < 10; i++) {
      sleep(100);
      customizedView = accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME));
      if (customizedView == null) {
        break;
      }
    }

    Assert.assertNull(customizedView,
        "customized view for TestDB0 should be removed, but was: " + customizedView);

    // clean up
    controller.syncStop();
    for (i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
