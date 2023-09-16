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
import org.apache.helix.ZkTestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.CustomizedState;
import org.apache.helix.model.CustomizedStateConfig;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test compute and clean customized view - if customized state is remove externally, controller
 * should remove the
 * orphan customized view
 */
public class TestComputeAndCleanupCustomizedView extends ZkTestBase {

  private final String RESOURCE_NAME = "TestDB0";
  private final String PARTITION_NAME1 = "TestDB0_0";
  private final String PARTITION_NAME2 = "TestDB0_1";
  private final String CUSTOMIZED_STATE_NAME1 = "customizedState1";
  private final String CUSTOMIZED_STATE_NAME2 = "customizedState2";
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

    // add CUSTOMIZED_STATE_NAME2 to aggregation enabled types
    CustomizedStateConfig config = new CustomizedStateConfig();
    List<String> aggregationEnabledTypes = new ArrayList<>();
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME2);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    // set INSTANCE1 to "STARTED" for CUSTOMIZED_STATE_NAME1
    CustomizedState customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME1, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME1, CUSTOMIZED_STATE_NAME1, RESOURCE_NAME),
        customizedState);

    // verify the customized view is empty for CUSTOMIZED_STATE_NAME1
    Boolean result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
        if (customizedView == null) {
          return true;
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String
        .format("Customized view should not have state for" + " resource: %s, partition: %s",
            RESOURCE_NAME, PARTITION_NAME1));

    // add CUSTOMIZED_STATE_NAME1 to aggregation enabled types
    aggregationEnabledTypes.add(CUSTOMIZED_STATE_NAME1);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    // verify the customized view should have "STARTED" for CUSTOMIZED_STATE_NAME1 for INSTANCE1,
    // but no state for INSTANCE2
    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
        if (customizedView != null) {
          Map<String, String> stateMap = customizedView.getRecord().getMapField(PARTITION_NAME1);
          return (stateMap.get(INSTANCE_NAME1).equals("STARTED"));
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String.format(
        "Customized view should have the state as STARTED for" + " instance: %s,"
            + " resource: %s, partition: %s and state: %s", INSTANCE_NAME1, RESOURCE_NAME,
        PARTITION_NAME1, CUSTOMIZED_STATE_NAME1));

    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
        if (customizedView != null) {
          Map<String, String> stateMap = customizedView.getRecord().getMapField(PARTITION_NAME1);
          return !stateMap.containsKey(INSTANCE_NAME2);
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String
        .format("Customized view should not have state for instance: " + "%s", INSTANCE_NAME2));

    // set INSTANCE2 to "STARTED" for CUSTOMIZED_STATE_NAME1
    customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME1, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME2, CUSTOMIZED_STATE_NAME1, RESOURCE_NAME),
        customizedState);

    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
        if (customizedView != null) {
          Map<String, String> stateMap = customizedView.getRecord().getMapField(PARTITION_NAME1);
          if (stateMap.containsKey(INSTANCE_NAME2)) {
            return (stateMap.get(INSTANCE_NAME1).equals("STARTED") && stateMap.get(INSTANCE_NAME2)
                .equals("STARTED"));
          }
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String.format(
        "Customized view should have both instances state " + "as STARTED for"
            + " resource: %s, partition: %s and state: %s", RESOURCE_NAME, PARTITION_NAME1,
        CUSTOMIZED_STATE_NAME1));

    // set INSTANCE2 to "STARTED" for CUSTOMIZED_STATE_NAME2
    customizedState = new CustomizedState(RESOURCE_NAME);
    customizedState.setState(PARTITION_NAME2, "STARTED");
    accessor.setProperty(
        keyBuilder.customizedState(INSTANCE_NAME2, CUSTOMIZED_STATE_NAME2, RESOURCE_NAME),
        customizedState);

    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME2, RESOURCE_NAME));
        if (customizedView != null) {
          Map<String, String> stateMap = customizedView.getRecord().getMapField(PARTITION_NAME2);
          return (stateMap.get(INSTANCE_NAME2).equals("STARTED"));
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String.format(
        "Customized view should have state " + "as STARTED " + "for instance: %s, "
            + " resource: %s, partition: %s and state: %s", INSTANCE_NAME2, RESOURCE_NAME,
        PARTITION_NAME2, CUSTOMIZED_STATE_NAME2));

    // remove CUSTOMIZED_STATE_NAME1 from aggregation enabled types
    aggregationEnabledTypes.remove(CUSTOMIZED_STATE_NAME1);
    config.setAggregationEnabledTypes(aggregationEnabledTypes);
    accessor.setProperty(keyBuilder.customizedStateConfig(), config);

    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
        if (customizedView == null) {
          return true;
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result,
        String.format("Customized view should not have state %s", CUSTOMIZED_STATE_NAME1));

    // disable controller
    ZKHelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableCluster(clusterName, false);
    ZkTestHelper.tryWaitZkEventsCleaned(controller.getZkClient());

    // drop resource
    admin.dropResource(clusterName, RESOURCE_NAME);

    // delete customized state manually, controller shall remove customized view when cluster is
    //enabled again

    accessor.removeProperty(
        keyBuilder.customizedState(INSTANCE_NAME1, CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));
    accessor.removeProperty(
        keyBuilder.currentState(INSTANCE_NAME2, CUSTOMIZED_STATE_NAME1, RESOURCE_NAME));

    // re-enable controller shall remove orphan external view
    // System.out.println("re-enabling controller");
    admin.enableCluster(clusterName, true);

    result = TestHelper.verify(new TestHelper.Verifier() {
      @Override
      public boolean verify() {
        CustomizedView customizedView =
            accessor.getProperty(keyBuilder.customizedView(CUSTOMIZED_STATE_NAME1));
        if (customizedView == null) {
          return true;
        }
        return false;
      }
    }, 12000);

    Thread.sleep(50);
    Assert.assertTrue(result, String
        .format("customized view for should be null for  resource: %s, partition: %s and state: %s",
            RESOURCE_NAME, PARTITION_NAME1, CUSTOMIZED_STATE_NAME1));

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}