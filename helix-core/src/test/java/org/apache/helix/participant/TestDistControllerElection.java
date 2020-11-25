package org.apache.helix.participant;

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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.manager.zk.DistributedLeaderElection;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestDistControllerElection extends ZkUnitTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestDistControllerElection.class);

  @Test()
  public void testController() throws Exception {
    System.out.println("START TestDistControllerElection at "
        + new Date(System.currentTimeMillis()));
    String className = getShortClassName();

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testController";

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "controller_0";
    HelixManager manager =
        new MockZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, _gZkClient);
    GenericHelixController controller0 = new GenericHelixController();

    List<HelixTimerTask> timerTasks = Collections.emptyList();
    DistributedLeaderElection election =
        new DistributedLeaderElection(manager, controller0, timerTasks);
    NotificationContext context = new NotificationContext(manager);
    try {
      context.setType(NotificationContext.Type.INIT);
      election.onControllerChange(context);

      // path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
      // ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
      LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
      AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
      // AssertJUnit.assertNotNull(election.getController());
      // AssertJUnit.assertNull(election.getLeader());
    } finally {
      manager.disconnect();
      controller0.shutdown();
    }

    manager =
        new MockZKHelixManager(clusterName, "controller_1", InstanceType.CONTROLLER, _gZkClient);
    GenericHelixController controller1 = new GenericHelixController();
    election = new DistributedLeaderElection(manager, controller1, timerTasks);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    try {
      election.onControllerChange(context);
      // leaderRecord = _gZkClient.<ZNRecord> readData(path);
      LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
      AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
      // AssertJUnit.assertNull(election.getController());
      // AssertJUnit.assertNull(election.getLeader());
    } finally {
      manager.disconnect();
      controller1.shutdown();
      accessor.removeProperty(keyBuilder.controllerLeader());
      TestHelper.dropCluster(clusterName, _gZkClient);
    }

    System.out.println("END TestDistControllerElection at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testController")
  public void testControllerParticipant() throws Exception {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName =
        CONTROLLER_CLUSTER_PREFIX + "_" + className + "_" + "testControllerParticipant";

    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "controller_0";
    HelixManager manager =
        new MockZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER_PARTICIPANT,
            _gZkClient);
    GenericHelixController controller0 = new GenericHelixController();
    List<HelixTimerTask> timerTasks = Collections.emptyList();

    DistributedLeaderElection election =
        new DistributedLeaderElection(manager, controller0, timerTasks);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    try {
      election.onControllerChange(context);

      LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
      AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

      // path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
      // ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
      // AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
      // AssertJUnit.assertNotNull(election.getController());
      // AssertJUnit.assertNotNull(election.getLeader());
    }
    finally {
      manager.disconnect();
      controller0.shutdown();
    }

    manager =
        new MockZKHelixManager(clusterName, "controller_1", InstanceType.CONTROLLER_PARTICIPANT,
            _gZkClient);
    GenericHelixController controller1 = new GenericHelixController();
    election = new DistributedLeaderElection(manager, controller1, timerTasks);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    try {
      election.onControllerChange(context);

      LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
      AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

      // leaderRecord = _gZkClient.<ZNRecord> readData(path);
      // AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
      // AssertJUnit.assertNull(election.getController());
      // AssertJUnit.assertNull(election.getLeader());
    } finally {
      manager.disconnect();
      controller1.shutdown();
    }
    accessor.removeProperty(keyBuilder.controllerLeader());

    TestHelper.dropCluster(clusterName, _gZkClient);
    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test(dependsOnMethods = "testController")
  public void testParticipant() throws Exception {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testParticipant";
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "participant_0";
    HelixManager manager =
        new MockZKHelixManager(clusterName, controllerName, InstanceType.PARTICIPANT, _gZkClient);
    GenericHelixController participant0 = new GenericHelixController();
    List<HelixTimerTask> timerTasks = Collections.emptyList();

    try {
      DistributedLeaderElection election =
          new DistributedLeaderElection(manager, participant0, timerTasks);
      Assert.fail(
          "Should not be able construct DistributedLeaderElection object using participant manager.");
    } catch (HelixException ex) {
      // expected
    }

    participant0.shutdown();
    manager.disconnect();
    TestHelper.dropCluster(clusterName, _gZkClient);
  }

  @Test(dependsOnMethods = "testController")
  public void testCompeteLeadership() throws Exception {
    final int managerCount = 3;
    String className = getShortClassName();
    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testCompeteLeadership";
    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    // Create controller leaders
    final Map<String, ZKHelixManager> managerList = new HashMap<>();
    final List<GenericHelixController> controllers = new ArrayList<>();
    for (int i = 0; i < managerCount; i++) {
      String controllerName = "controller_" + i;
      ZKHelixManager manager =
          new ZKHelixManager(clusterName, controllerName, InstanceType.CONTROLLER, ZK_ADDR);
      GenericHelixController controller0 = new GenericHelixController();
      DistributedLeaderElection election =
          new DistributedLeaderElection(manager, controller0, Collections.EMPTY_LIST);
      controllers.add(controller0);
      manager.connect();
      managerList.put(manager.getInstanceName(), manager);
    }

    try {
      // Remove leader manager one by one, and verify if the leader node exists
      while (!managerList.isEmpty()) {
        // Ensure a controller successfully acquired leadership.
        Assert.assertTrue(TestHelper.verify(new TestHelper.Verifier() {
          @Override
          public boolean verify() {
            LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
            if (liveInstance != null) {
              // disconnect the current leader manager
              managerList.remove(liveInstance.getInstanceName()).disconnect();
              return true;
            } else {
              return false;
            }
          }
        }, 1000));
      }
    } finally {
      for (GenericHelixController controller : controllers) {
        controller.shutdown();
      }
      for (ZKHelixManager mgr: managerList.values()) {
        mgr.disconnect();
      }
      TestHelper.dropCluster(clusterName, _gZkClient);
    }
  }
}
