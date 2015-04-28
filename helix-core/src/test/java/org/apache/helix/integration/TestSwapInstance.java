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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.model.IdealState;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSwapInstance extends ZkStandAloneCMTestBase {
  @Test
  public void TestSwap() throws Exception {
    HelixManager manager = _controller;
    HelixDataAccessor helixAccessor = manager.getHelixDataAccessor();
    _setupTool.addResourceToCluster(CLUSTER_NAME, "MyDB", 64, STATE_MODEL);
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, "MyDB", _replica);

    ZNRecord idealStateOld1 = new ZNRecord("TestDB");
    ZNRecord idealStateOld2 = new ZNRecord("MyDB");

    IdealState is1 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("TestDB"));
    idealStateOld1.merge(is1.getRecord());

    IdealState is2 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("MyDB"));
    idealStateOld2.merge(is2.getRecord());

    String instanceName = "localhost_" + (START_PORT + 0);
    _setupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, instanceName, false);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    String instanceName2 = "localhost_" + (START_PORT + 444);
    _setupTool.addInstanceToCluster(CLUSTER_NAME, instanceName2);

    boolean exception = false;
    try {
      _setupTool.swapInstance(CLUSTER_NAME, instanceName, instanceName2);
    } catch (Exception e) {
      exception = true;
    }
    Assert.assertTrue(exception);

    _participants[0].syncStop();
    Thread.sleep(1000);

    exception = false;
    try {
      _setupTool.swapInstance(CLUSTER_NAME, instanceName, instanceName2);
    } catch (Exception e) {
      e.printStackTrace();
      exception = true;
    }
    Assert.assertFalse(exception);
    MockParticipant newParticipant =
        new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName2);
    newParticipant.syncStart();

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    is1 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("TestDB"));

    is2 = helixAccessor.getProperty(helixAccessor.keyBuilder().idealStates("MyDB"));

    for (String key : idealStateOld1.getMapFields().keySet()) {
      for (String host : idealStateOld1.getMapField(key).keySet()) {
        if (host.equals(instanceName)) {
          Assert.assertTrue(idealStateOld1.getMapField(key).get(instanceName)
              .equals(is1.getRecord().getMapField(key).get(instanceName2)));
        } else {
          Assert.assertTrue(idealStateOld1.getMapField(key).get(host)
              .equals(is1.getRecord().getMapField(key).get(host)));
        }
      }
    }

    for (String key : idealStateOld1.getListFields().keySet()) {
      Assert.assertEquals(idealStateOld1.getListField(key).size(), is1.getRecord()
          .getListField(key).size());
      for (int i = 0; i < idealStateOld1.getListField(key).size(); i++) {
        String host = idealStateOld1.getListField(key).get(i);
        String newHost = is1.getRecord().getListField(key).get(i);
        if (host.equals(instanceName)) {
          Assert.assertTrue(newHost.equals(instanceName2));
        } else {
          // System.out.println(key + " " + i+ " " + host + " "+newHost);
          // System.out.println(idealStateOld1.getListField(key));
          // System.out.println(is1.getRecord().getListField(key));

          Assert.assertTrue(host.equals(newHost));
        }
      }
    }
  }
}
