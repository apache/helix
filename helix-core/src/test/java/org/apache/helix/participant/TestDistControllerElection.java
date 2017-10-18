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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.manager.zk.DistributedLeaderElection;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    String path = "/" + clusterName;
    if (_gZkClient.exists(path)) {
      _gZkClient.deleteRecursive(path);
    }

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
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    // path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
    // ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
    // AssertJUnit.assertNotNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    manager =
        new MockZKHelixManager(clusterName, "controller_1", InstanceType.CONTROLLER, _gZkClient);
    GenericHelixController controller1 = new GenericHelixController();
    election = new DistributedLeaderElection(manager, controller1, timerTasks);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);
    // leaderRecord = _gZkClient.<ZNRecord> readData(path);
    liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    System.out.println("END TestDistControllerElection at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testControllerParticipant() throws Exception {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName =
        CONTROLLER_CLUSTER_PREFIX + "_" + className + "_" + "testControllerParticipant";
    String path = "/" + clusterName;
    if (_gZkClient.exists(path)) {
      _gZkClient.deleteRecursive(path);
    }

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
    election.onControllerChange(context);

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // path = PropertyPathConfig.getPath(PropertyType.LEADER, clusterName);
    // ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path);
    // AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    // AssertJUnit.assertNotNull(election.getController());
    // AssertJUnit.assertNotNull(election.getLeader());

    manager =
        new MockZKHelixManager(clusterName, "controller_1", InstanceType.CONTROLLER_PARTICIPANT,
            _gZkClient);
    GenericHelixController controller1 = new GenericHelixController();
    election = new DistributedLeaderElection(manager, controller1, timerTasks);
    context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.CALLBACK);
    election.onControllerChange(context);

    liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // leaderRecord = _gZkClient.<ZNRecord> readData(path);
    // AssertJUnit.assertEquals(controllerName, leaderRecord.getSimpleField("LEADER"));
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());

    LOG.info("END " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testParticipant() throws Exception {
    String className = getShortClassName();
    LOG.info("RUN " + className + " at " + new Date(System.currentTimeMillis()));

    final String clusterName = CLUSTER_PREFIX + "_" + className + "_" + "testParticipant";
    String path = "/" + clusterName;
    if (_gZkClient.exists(path)) {
      _gZkClient.deleteRecursive(path);
    }
    TestHelper.setupEmptyCluster(_gZkClient, clusterName);

    final String controllerName = "participant_0";
    HelixManager manager =
        new MockZKHelixManager(clusterName, controllerName, InstanceType.PARTICIPANT, _gZkClient);
    GenericHelixController participant0 = new GenericHelixController();
    List<HelixTimerTask> timerTasks = Collections.emptyList();

    DistributedLeaderElection election =
        new DistributedLeaderElection(manager, participant0, timerTasks);
    NotificationContext context = new NotificationContext(manager);
    context.setType(NotificationContext.Type.INIT);
    election.onControllerChange(context);

    path = PropertyPathBuilder.controllerLeader(clusterName);
    ZNRecord leaderRecord = _gZkClient.<ZNRecord> readData(path, true);
    AssertJUnit.assertNull(leaderRecord);
    // AssertJUnit.assertNull(election.getController());
    // AssertJUnit.assertNull(election.getLeader());
  }

}
