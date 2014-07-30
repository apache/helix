package org.apache.helix.manager.zk;

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

import org.apache.helix.TestHelper;
import org.apache.helix.ZkTestHelper;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHandleNewSession extends ZkTestBase {
  @Test
  public void testHandleNewSession() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    MockParticipant participant =
        new MockParticipant(_zkaddr, clusterName, "localhost_12918");
    participant.syncStart();

    // Logger.getRootLogger().setLevel(Level.INFO);
    String lastSessionId = participant.getSessionId();
    for (int i = 0; i < 3; i++) {
      // System.err.println("curSessionId: " + lastSessionId);
      ZkTestHelper.expireSession(participant.getZkClient());

      String sessionId = participant.getSessionId();
      Assert.assertTrue(sessionId.compareTo(lastSessionId) > 0,
          "Session id should be increased after expiry");
      lastSessionId = sessionId;

      // make sure session id is not 0
      Assert.assertFalse(sessionId.equals("0"),
          "Hit race condition in zhclient.handleNewSession(). sessionId is not returned yet.");

      // TODO: need to test session expiry during handleNewSession()
    }

    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("Disconnecting ...");
    participant.syncStop();

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
