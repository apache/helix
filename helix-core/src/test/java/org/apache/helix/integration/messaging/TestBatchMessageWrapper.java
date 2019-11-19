package org.apache.helix.integration.messaging;

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
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.messaging.handling.BatchMessageWrapper;
import org.apache.helix.mock.participant.MockMSModelFactory;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBatchMessageWrapper extends ZkUnitTestBase {
  class MockBatchMsgWrapper extends BatchMessageWrapper {
    int _startCount = 0;
    int _endCount = 0;

    @Override
    public void start(Message batchMsg, NotificationContext context) {
      // System.out.println("test batchMsg.start() invoked, " + batchMsg.getTgtName());
      _startCount++;
    }

    @Override
    public void end(Message batchMsg, NotificationContext context) {
      // System.out.println("test batchMsg.end() invoked, " + batchMsg.getTgtName());
      _endCount++;
    }
  }

  class TestMockMSModelFactory extends MockMSModelFactory {
    @Override
    public BatchMessageWrapper createBatchMessageWrapper(String resourceName) {
      return new MockBatchMsgWrapper();
    }
  }

  @Test
  public void testBasic() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    final int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // startPort
        "localhost", // participantNamePrefix
        "TestDB", // resourceNamePrefix
        1, // resourceNb
        2, // partitionNb
        n, // nodesNb
        2, // replica
        "MasterSlave", true);

    // enable batch-message
    ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, new ZkBaseDataAccessor<ZNRecord>(_gZkClient));
    Builder keyBuilder = accessor.keyBuilder();
    IdealState idealState = accessor.getProperty(keyBuilder.idealStates("TestDB0"));
    idealState.setBatchMessageMode(true);
    accessor.setProperty(keyBuilder.idealStates("TestDB0"), idealState);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    TestMockMSModelFactory[] ftys = new TestMockMSModelFactory[n];

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      ftys[i] = new TestMockMSModelFactory();
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].getStateMachineEngine().registerStateModelFactory("MasterSlave", ftys[i]);
      participants[i].syncStart();
      int finalI = i;
      TestHelper.verify(() -> participants[finalI].isConnected()
          && accessor.getChildNames(keyBuilder.liveInstances())
              .contains(participants[finalI].getInstanceName()),
          TestHelper.WAIT_DURATION);

      // wait for each participant to complete state transitions, so we have deterministic results
      ZkHelixClusterVerifier _clusterVerifier =
          new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
      Assert.assertTrue(_clusterVerifier.verifyByPolling(),
          "participant: " + instanceName + " fails to complete all transitions");
    }

    // check batch-msg-wrapper counts
    MockBatchMsgWrapper wrapper = (MockBatchMsgWrapper) ftys[0].getBatchMessageWrapper("TestDB0");
    // System.out.println("startCount: " + wrapper._startCount);
    Assert.assertEquals(wrapper._startCount, 3,
        "Expect 3 batch.start: O->S, S->M, and M->S for 1st participant");
    // System.out.println("endCount: " + wrapper._endCount);
    Assert.assertEquals(wrapper._endCount, 3,
        "Expect 3 batch.end: O->S, S->M, and M->S for 1st participant");

    wrapper = (MockBatchMsgWrapper) ftys[1].getBatchMessageWrapper("TestDB0");
    // System.out.println("startCount: " + wrapper._startCount);
    Assert.assertEquals(wrapper._startCount, 2,
        "Expect 2 batch.start: O->S and S->M for 2nd participant");
    // System.out.println("endCount: " + wrapper._endCount);
    Assert.assertEquals(wrapper._startCount, 2,
        "Expect 2 batch.end: O->S and S->M for 2nd participant");

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    TestHelper.dropCluster(clusterName, _gZkClient);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

}
