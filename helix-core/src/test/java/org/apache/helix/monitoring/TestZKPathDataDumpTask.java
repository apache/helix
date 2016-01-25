package org.apache.helix.monitoring;

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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Date;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.Error;
import org.apache.helix.model.StatusUpdate;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestZKPathDataDumpTask extends ZkUnitTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 1;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    BaseDataAccessor<ZNRecord> baseAccessor = accessor.getBaseDataAccessor();

    HelixManager manager = mock(HelixManager.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(manager.getClusterName()).thenReturn(clusterName);

    // run dump task without statusUpdates and errors, should not remove any existing
    // statusUpdate/error paths
    ZKPathDataDumpTask task = new ZKPathDataDumpTask(manager, 0L, 0L, Integer.MAX_VALUE);
    task.run();
    PropertyKey controllerStatusUpdateKey = keyBuilder.controllerTaskStatuses();
    Assert.assertTrue(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    PropertyKey controllerErrorKey = keyBuilder.controllerTaskErrors();
    Assert.assertTrue(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    PropertyKey statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918");
    Assert.assertTrue(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    PropertyKey errorKey = keyBuilder.stateTransitionErrors("localhost_12918");

    // add participant status updates and errors
    statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918", "session_0", "TestDB0", "TestDB0_0");
    accessor.setInstanceStatusUpdate(
        "localhost_12918", "session_0", "TestDB0", new StatusUpdate(new ZNRecord("TestDB0_0")));
    errorKey = keyBuilder.stateTransitionError("localhost_12918", "session_0", "TestDB0", "TestDB0_0");
    accessor.setInstanceError("localhost_12918", "session_0", "TestDB0", new Error(new ZNRecord("TestDB0_0")));

    // add controller status updates and errors
    controllerStatusUpdateKey = keyBuilder.controllerTaskStatus("session_0", "TestDB");
    accessor.setControllerStatusUpdate("session_0", new StatusUpdate(new ZNRecord("TestDB")));
    controllerErrorKey = keyBuilder.controllerTaskError("TestDB_error");
    accessor.setControllerError(new Error(new ZNRecord("TestDB_error")));

    // run dump task, should remove existing statusUpdate/error paths
    task.run();
    Assert.assertFalse(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(errorKey.getPath(), 0));

    controllerStatusUpdateKey = keyBuilder.controllerTaskStatuses();
    Assert.assertTrue(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    controllerErrorKey = keyBuilder.controllerTaskErrors();
    Assert.assertTrue(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918");
    Assert.assertTrue(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    errorKey = keyBuilder.stateTransitionErrors("localhost_12918");

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test
  public void testCapacityReached() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 1;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    BaseDataAccessor<ZNRecord> baseAccessor = accessor.getBaseDataAccessor();

    HelixManager manager = mock(HelixManager.class);
    when(manager.getHelixDataAccessor()).thenReturn(accessor);
    when(manager.getClusterName()).thenReturn(clusterName);

    // run dump task without statusUpdates and errors, should not remove any existing
    // statusUpdate/error paths
    ZKPathDataDumpTask task = new ZKPathDataDumpTask(manager, Long.MAX_VALUE, Long.MAX_VALUE, 1);
    task.run();
    PropertyKey controllerStatusUpdateKey = keyBuilder.controllerTaskStatuses();
    Assert.assertTrue(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    PropertyKey controllerErrorKey = keyBuilder.controllerTaskErrors();
    Assert.assertTrue(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    PropertyKey statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918");
    Assert.assertTrue(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    PropertyKey errorKey = keyBuilder.stateTransitionErrors("localhost_12918");
    Assert.assertTrue(baseAccessor.exists(errorKey.getPath(), 0));

    // add participant status updates and errors
    statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918", "session_0", "TestDB0", "TestDB0_0");
    accessor.setInstanceStatusUpdate(
        "localhost_12918", "session_0", "TestDB0", new StatusUpdate(new ZNRecord("TestDB0_0")));
    errorKey = keyBuilder.stateTransitionError("localhost_12918", "session_0", "TestDB0", "TestDB0_0");
    accessor.setInstanceError("localhost_12918", "session_0", "TestDB0", new Error(new ZNRecord("TestDB0_0")));

    // add controller status updates and errors (one of each, should not trigger anything)
    controllerStatusUpdateKey = keyBuilder.controllerTaskStatus("session_0", "TestDB");
    accessor.setControllerStatusUpdate("session_0", new StatusUpdate(new ZNRecord("TestDB")));
    controllerErrorKey = keyBuilder.controllerTaskError("TestDB_error");
    accessor.setControllerError(new Error(new ZNRecord("TestDB_error")));

    // run dump task, should not remove anything because the threshold is not exceeded
    task.run();
    Assert.assertTrue(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    Assert.assertTrue(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    Assert.assertTrue(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    Assert.assertTrue(baseAccessor.exists(errorKey.getPath(), 0));

    // add a second set of all status updates and errors
    statusUpdateKey = keyBuilder.stateTransitionStatus("localhost_12918", "session_0", "TestDB0", "TestDB0_1");
    accessor.setInstanceStatusUpdate(
        "localhost_12918", "session_0", "TestDB0", new StatusUpdate(new ZNRecord("TestDB0_1")));
    errorKey = keyBuilder.stateTransitionError("localhost_12918", "session_0", "TestDB0", "TestDB0_1");
    accessor.setInstanceError("localhost_12918", "session_0", "TestDB0", new Error(new ZNRecord("TestDB0_1")));
    controllerStatusUpdateKey = keyBuilder.controllerTaskStatus("session_0", "TestDB1");
    accessor.setControllerStatusUpdate("session_0", new StatusUpdate(new ZNRecord("TestDB1")));
    controllerErrorKey = keyBuilder.controllerTaskError("TestDB1_error");
    accessor.setControllerError(new Error(new ZNRecord("TestDB1_error")));

    // run dump task, should remove everything since capacities are exceeded
    task.run();
    Assert.assertFalse(baseAccessor.exists(controllerStatusUpdateKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(controllerErrorKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(statusUpdateKey.getPath(), 0));
    Assert.assertFalse(baseAccessor.exists(errorKey.getPath(), 0));
  }
}
