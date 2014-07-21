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

import org.apache.helix.HelixConnection;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixParticipant;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.integration.TestHelixConnection;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkHelixParticipant extends ZkTestBase {

  @Test
  public void testOnConnectedAndDisconnecting() throws Exception {
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
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    // create connection
    HelixConnection connection = new ZkHelixConnection(_zkaddr);
    connection.connect();

    // start participant
    ClusterId clusterId = ClusterId.from(clusterName);
    HelixParticipant[] participants = new HelixParticipant[n];
    for (int i = 0; i < n; i++) {
      int port = 12918 + i;
      ParticipantId participantId = ParticipantId.from("localhost_" + port);

      participants[i] = connection.createParticipant(clusterId, participantId);
      participants[i].getStateMachineEngine().registerStateModelFactory(
          StateModelDefId.from("MasterSlave"), new TestHelixConnection.MockStateModelFactory());

      participants[i].start();
    }

    // check live-instance znode for localhost_12918/12919 exist
    HelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    for (int i = 0; i < n; i++) {
      Assert.assertNotNull(accessor.getProperty(keyBuilder.liveInstance(participants[i]
          .getParticipantId().stringify())));
    }

    // stop participant localhost_12918
    participants[0].stop();

    // check live-instance znode for localhost_12918 is gone
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(participants[0]
        .getParticipantId().stringify())));

    // clean up
    connection.disconnect();

    // check live-instance znode for localhost_12919 is gone
    Assert.assertNull(accessor.getProperty(keyBuilder.liveInstance(participants[1]
        .getParticipantId().stringify())));

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
