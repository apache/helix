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
import java.util.List;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAddStateModelFactoryAfterConnect extends ZkTestBase {
  @Test
  public void testBasic() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 5;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipant[] participants = new MockParticipant[n];

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        n, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // add a new idealState without registering message handling factory
    _setupTool.addResourceToCluster(clusterName, "TestDB1", 16, "MasterSlave");

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB1"));
    idealState.setStateModelFactoryId(StateModelFactoryId.from("TestDB1_Factory"));
    accessor.setProperty(keyBuilder.idealStates("TestDB1"), idealState);
    _setupTool.rebalanceStorageCluster(clusterName, "TestDB1", 3);

    // assert that we have received OFFLINE->SLAVE messages for all partitions
    int totalMsgs = 0;
    for (int retry = 0; retry < 5; retry++) {
      Thread.sleep(100);
      totalMsgs = 0;
      for (int i = 0; i < n; i++) {
        List<Message> msgs =
            accessor.getChildValues(keyBuilder.messages(participants[i].getInstanceName()));
        totalMsgs += msgs.size();
      }

      if (totalMsgs == 48) // partition# x replicas
        break;
    }

    Assert
        .assertEquals(
            totalMsgs,
            48,
            "Should accumulated 48 unprocessed messages (1 O->S per partition per replica) because TestDB1 is added without state-model-factory but was "
                + totalMsgs);

    // register "TestDB1_Factory" state model factory
    // Logger.getRootLogger().setLevel(Level.INFO);
    for (int i = 0; i < n; i++) {
      participants[i].getStateMachineEngine().registerStateModelFactory(StateModelDefId.MasterSlave,
          "TestDB1_Factory", new MockMSModelFactory());
    }

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
