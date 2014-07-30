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

import java.util.Date;

import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test clean external-view - if current-state is remove externally, controller should remove the
 * orphan external-view
 */
public class TestCleanupExternalView extends ZkTestBase {
  @Test
  public void test() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        2, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipant[] participants = new MockParticipant[n];
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(_zkaddr,
                clusterName));
    Assert.assertTrue(result);

    // disable controller
    ZKHelixAdmin admin = new ZKHelixAdmin(_zkclient);
    admin.enableCluster(clusterName, false);
    // wait all pending zk-events being processed, otherwise remove current-state will cause
    // controller send O->S message
    ZkTestHelper.tryWaitZkEventsCleaned(controller.getZkClient());
    // System.out.println("paused controller");

    // drop resource
    admin.dropResource(clusterName, "TestDB0");

    // delete current-state manually, controller shall remove external-view when cluster is enabled
    // again
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    // System.out.println("remove current-state");
    LiveInstance liveInstance = accessor.getProperty(keyBuilder.liveInstance("localhost_12918"));
    accessor.removeProperty(keyBuilder.currentState("localhost_12918", liveInstance
        .getTypedSessionId().stringify(), "TestDB0"));
    liveInstance = accessor.getProperty(keyBuilder.liveInstance("localhost_12919"));
    accessor.removeProperty(keyBuilder.currentState("localhost_12919", liveInstance
        .getTypedSessionId().stringify(), "TestDB0"));

    // re-enable controller shall remove orphan external-view
    // System.out.println("re-enabling controller");
    admin.enableCluster(clusterName, true);

    ExternalView externalView = null;
    for (int i = 0; i < 10; i++) {
      Thread.sleep(100);
      externalView = accessor.getProperty(keyBuilder.externalView("TestDB0"));
      // System.out.println("externalView: " + externalView);
      if (externalView == null) {
        break;
      }
    }

    Assert.assertNull(externalView, "external-view for TestDB0 should be removed, but was: "
        + externalView);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
