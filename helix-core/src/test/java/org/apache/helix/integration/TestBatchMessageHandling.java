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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.mock.participant.MockMSStateModel;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.FullAutoModeISBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBatchMessageHandling extends ZkStandAloneCMTestBase {

  @Test
  public void testSubMessageFailed() throws InterruptedException {
    TestOnlineOfflineStateModel._numOfSuccessBeforeFailure.set(6);

    // Let one instance handle all the batch messages.
    _participants[0].getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new TestOnlineOfflineStateModelFactory(), "TestFactory");
    for (int i = 1; i < _participants.length; i++) {
      _participants[i].syncStop();
    }

    HelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    // Check that the Participants really stopped
    for (int i = 1; i < _participants.length; i++) {
      List<String> liveInstances =
          dataAccessor.getChildNames(dataAccessor.keyBuilder().liveInstances());
      if (_participants[i].isConnected()
          || liveInstances.contains(_participants[i].getInstanceName())) {
        Thread.sleep(1000L);
      }
    }

    // Add 1 db with batch message enabled. Each db has 10 partitions.
    // So it will have 1 batch message and 10 sub messages.
    String dbName = "TestDBSubMessageFail";
    IdealState idealState = new FullAutoModeISBuilder(dbName).setStateModel("OnlineOffline")
        .setStateModelFactoryName("TestFactory").setNumPartitions(10).setNumReplica(1).build();
    idealState.setBatchMessageMode(true);
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, dbName, idealState);

    // Check that IdealState has really been added
    for (int i = 0; i < 5; i++) {
      IdealState is =
          _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, dbName);
      if (!idealState.equals(is)) {
        Thread.sleep(1000L);
      }
    }
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, 1);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Thread.sleep(2000L);

    int numOfOnlines = 0;
    int numOfErrors = 0;
    ExternalView externalView =
        _gSetupTool.getClusterManagementTool().getResourceExternalView(CLUSTER_NAME, dbName);
    for (String partition : externalView.getPartitionSet()) {
      if (externalView.getStateMap(partition).values().contains("ONLINE")) {
        numOfOnlines++;
      }
      if (externalView.getStateMap(partition).values().contains("ERROR")) {
        numOfErrors++;
      }
    }
    if (numOfErrors != 4 || numOfOnlines != 6) {
      System.out.println("IdealState: " + idealState);
      System.out.println("ExternalView: " + externalView);
    }
    Assert.assertEquals(numOfErrors, 4);
    Assert.assertEquals(numOfOnlines, 6);
  }

  public static class TestOnlineOfflineStateModelFactory
      extends StateModelFactory<TestOnlineOfflineStateModel> {
    @Override
    public TestOnlineOfflineStateModel createNewStateModel(String resourceName,
        String stateUnitKey) {
      return new TestOnlineOfflineStateModel();
    }
  }

  public static class TestOnlineOfflineStateModel extends StateModel {
    private static Logger LOG = LoggerFactory.getLogger(MockMSStateModel.class);
    static AtomicInteger _numOfSuccessBeforeFailure = new AtomicInteger();

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      if (_numOfSuccessBeforeFailure.getAndDecrement() > 0) {
        LOG.info("State transition from Offline to Online");
        return;
      }
      throw new HelixException("Number of Success reached");
    }

    public void onBecomeOfflineFromOnline(Message message, NotificationContext context) {
      LOG.info("State transition from Online to Offline");
    }

    public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
      LOG.info("State transition from Offline to Dropped");
    }
  }
}
