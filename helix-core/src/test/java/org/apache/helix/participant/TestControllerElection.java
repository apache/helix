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

import java.util.Date;

import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.MockMultiClusterController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkHelixLeaderElection;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestControllerElection extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestControllerElection.class);

  @Test()
  public void testController() throws Exception {
    String clusterName = TestUtil.getTestName();

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupEmptyCluster(_zkclient, clusterName);

    String controllerName = "controller_0";
    MockController controller = new MockController(_zkaddr, clusterName, controllerName);
    GenericHelixController pipeline = new GenericHelixController();

    ZkHelixLeaderElection leader = new ZkHelixLeaderElection(controller.getController(), pipeline);
    NotificationContext context = new NotificationContext(controller);
    context.setType(NotificationContext.Type.INIT);
    leader.onControllerChange(context);

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // Start another controller, leader should remain unchanged
    String controllerName1 = "controller_1";
    MockController controller1 = new MockController(_zkaddr, clusterName, controllerName1);
    GenericHelixController pipeline1 = new GenericHelixController();
    ZkHelixLeaderElection leader1 = new ZkHelixLeaderElection(controller1.getController(), pipeline1);
    NotificationContext context1 = new NotificationContext(controller1);
    context1.setType(NotificationContext.Type.INIT);
    leader1.onControllerChange(context1);
    liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // clean up
    controller1.getConn().disconnect();
    controller.getConn().disconnect();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test()
  public void testMultiClusterController() throws Exception {
    String clusterName = TestUtil.getTestName();
    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    TestHelper.setupEmptyCluster(_zkclient, clusterName);

    String controllerName = "controller_0";
    MockMultiClusterController controller = new MockMultiClusterController(_zkaddr, clusterName, controllerName);
    GenericHelixController pipeline = new GenericHelixController();

    ZkHelixLeaderElection leader = new ZkHelixLeaderElection(controller.getController(), pipeline);
    NotificationContext context = new NotificationContext(controller);
    context.setType(NotificationContext.Type.CALLBACK);
    leader.onControllerChange(context);

    LiveInstance liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // Start another controller, leader should remain unchanged
    MockMultiClusterController controller1 = new MockMultiClusterController(_zkaddr, clusterName, "controller_1");

    GenericHelixController pipeline1 = new GenericHelixController();
    ZkHelixLeaderElection leader1 = new ZkHelixLeaderElection(controller.getController(), pipeline1);
    context = new NotificationContext(controller);
    context.setType(NotificationContext.Type.CALLBACK);
    leader1.onControllerChange(context);
    liveInstance = accessor.getProperty(keyBuilder.controllerLeader());
    AssertJUnit.assertEquals(controllerName, liveInstance.getInstanceName());

    // clean up
    controller1.getConn().disconnect();
    controller.getConn().disconnect();
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
