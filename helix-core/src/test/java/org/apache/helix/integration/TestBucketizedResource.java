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

import java.util.Arrays;
import java.util.List;

import org.apache.helix.ExternalViewChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.NotificationContext;
import org.apache.helix.NotificationContext.Type;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.apache.helix.tools.DefaultIdealStateCalculator;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestBucketizedResource extends ZkTestBase {

  private void setupCluster(String clusterName, List<String> instanceNames, String dbName,
      int replica, int partitions, int bucketSize) {
    _gSetupTool.addCluster(clusterName, true);
    _gSetupTool.addInstancesToCluster(clusterName,
        instanceNames.toArray(new String[instanceNames.size()]));

    // add a bucketized resource
    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ZNRecord idealStateRec =
        DefaultIdealStateCalculator.calculateIdealState(instanceNames, partitions, replica - 1,
            dbName,
            "MASTER", "SLAVE");
    IdealState idealState = new IdealState(idealStateRec);
    idealState.setBucketSize(bucketSize);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas(Integer.toString(replica));
    accessor.setProperty(keyBuilder.idealStates(dbName), idealState);

  }

  @Test()
  public void testBucketizedResource() {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    List<String> instanceNames =
        Arrays.asList("localhost_12918", "localhost_12919", "localhost_12920", "localhost_12921", "localhost_12922");
    int n = instanceNames.size();
    String dbName = "TestDB0";

    MockParticipantManager[] participants = new MockParticipantManager[5];

    setupCluster(clusterName, instanceNames, dbName, 3, 10, 1);

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);

    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName);
    controller.syncStart();

    // start participants
    for (int i = 0; i < n; i++) {
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(i));
      participants[i].syncStart();
    }
    PropertyKey evKey = accessor.keyBuilder().externalView(dbName);

    BestPossibleExternalViewVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    ExternalView ev = accessor.getProperty(evKey);
    int v1 = ev.getRecord().getVersion();
    // disable the participant
    _gSetupTool.getClusterManagementTool().enableInstance(clusterName,
        participants[0].getInstanceName(), false);

    // wait for change in EV
    Assert.assertTrue(_clusterVerifier.verifyByPolling());


    // read the version in EV
    ev = accessor.getProperty(evKey);
    int v2 = ev.getRecord().getVersion();
    Assert.assertEquals(v2 > v1, true);

    // clean up
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);
  }

  @Test
  public void testBounceDisableAndDrop() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String dbName = "TestDB0";
    List<String> instanceNames =
        Arrays.asList("localhost_0", "localhost_1", "localhost_2", "localhost_3", "localhost_4");
    int n = instanceNames.size();

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    setupCluster(clusterName, instanceNames, dbName, 3, 10, 2);

    // start controller
    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName);
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(i));
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // bounce
    participants[0].syncStop();
    participants[0] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(0));
    participants[0].syncStart();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure participants[0]'s current state is bucketzied correctly during carryover
    String path =
        keyBuilder.currentState(instanceNames.get(0), participants[0].getSessionId(), dbName)
            .getPath();
    ZNRecord record = _baseAccessor.get(path, null, 0);
    Assert.assertTrue(record.getMapFields().size() == 0);

    // disable the bucketize resource
    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.enableResource(clusterName, dbName, false);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // drop the bucketize resource
    _gSetupTool.dropResourceFromCluster(clusterName, dbName);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // make sure external-view is cleaned up
    final String evPath = keyBuilder.externalView(dbName).getPath();

    TestHelper.verify(new TestHelper.Verifier() {
        @Override
        public boolean verify() {
          return !_baseAccessor.exists(evPath, 0);
        }
      }, 3000);

    boolean result = _baseAccessor.exists(evPath, 0);
    Assert.assertFalse(result);

    // clean up
    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    deleteCluster(clusterName);
  }

  class TestExternalViewListener implements ExternalViewChangeListener {
    int cbCnt = 0;

    @Override
    public void onExternalViewChange(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      if (changeContext.getType() == Type.CALLBACK) {
        cbCnt++;
      }
    }
  }

  @Test
  public void testListenerOnBucketizedResource() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    String dbName = "TestDB0";
    List<String> instanceNames =
        Arrays.asList("localhost_0", "localhost_1", "localhost_2", "localhost_3", "localhost_4");
    int n = instanceNames.size();

    ZKHelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, _baseAccessor);
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();

    setupCluster(clusterName, instanceNames, dbName, 3, 10, 2);

    // start controller
    ClusterControllerManager controller = new ClusterControllerManager(ZK_ADDR, clusterName);
    controller.syncStart();

    // start participants
    MockParticipantManager[] participants = new MockParticipantManager[n];
    for (int i = 0; i < n; i++) {
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceNames.get(i));
      participants[i].syncStart();
    }

    ZkHelixClusterVerifier _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // add an external view listener
    final TestExternalViewListener listener = new TestExternalViewListener();
    controller.addExternalViewChangeListener(listener);

    // remove "TestDB0"
    _gSetupTool.dropResourceFromCluster(clusterName, dbName);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    // wait callback to finish
    TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() throws Exception {
        return listener.cbCnt > 0;
      }
    }, 20000);
    Assert.assertTrue(listener.cbCnt > 0);

    listener.cbCnt = 0;

    // add a new db
    String newDbName = "TestDB1";
    int r = 3;
    ZNRecord idealStateRec =
        DefaultIdealStateCalculator.calculateIdealState(instanceNames, 10, r - 1, newDbName,
            "MASTER", "SLAVE");
    IdealState idealState = new IdealState(idealStateRec);
    idealState.setBucketSize(2);
    idealState.setStateModelDefRef("MasterSlave");
    idealState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    idealState.setReplicas(Integer.toString(r));
    accessor.setProperty(keyBuilder.idealStates(newDbName), idealState);

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    TestHelper.verify(new TestHelper.Verifier() {
      @Override public boolean verify() throws Exception {
        return listener.cbCnt > 0;
      }
    }, 20000);

    Assert.assertTrue(listener.cbCnt > 0);

    // clean up
    controller.syncStop();
    for (MockParticipantManager participant : participants) {
      participant.syncStop();
    }
    deleteCluster(clusterName);
  }
}
