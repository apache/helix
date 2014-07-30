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
import java.util.concurrent.atomic.AtomicBoolean;

import org.I0Itec.zkclient.IZkChildListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.PropertyPathConfig;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.Message;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.testutil.TestUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestMessageThrottle extends ZkTestBase {
  @Test()
  public void testMessageThrottle() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);

    String clusterName = TestUtil.getTestName();
    MockParticipant[] participants = new MockParticipant[5];

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, _zkaddr, 12918, // participant start
                                                         // port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        10, // partitions per resource
        5, // number of nodes
        3, // replicas
        "MasterSlave", true); // do rebalance

    // setup message constraint
    // "MESSAGE_TYPE=STATE_TRANSITION,TRANSITION=OFFLINE-SLAVE,INSTANCE=.*,CONSTRAINT_VALUE=1";
    HelixAdmin admin = new ZKHelixAdmin(_zkclient);
    ConstraintItemBuilder builder = new ConstraintItemBuilder();
    builder.addConstraintAttribute("MESSAGE_TYPE", "STATE_TRANSITION")
        .addConstraintAttribute("INSTANCE", ".*").addConstraintAttribute("CONSTRAINT_VALUE", "1");

    // Map<String, String> constraints = new TreeMap<String, String>();
    // constraints.put("MESSAGE_TYPE", "STATE_TRANSITION");
    // // constraints.put("TRANSITION", "OFFLINE-SLAVE");
    // constraints.put("CONSTRAINT_VALUE", "1");
    // constraints.put("INSTANCE", ".*");
    admin.setConstraint(clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint1",
        builder.build());

    final ZKHelixDataAccessor accessor =
        new ZKHelixDataAccessor(clusterName, _baseAccessor);

    // make sure we never see more than 1 state transition message for each participant
    final AtomicBoolean success = new AtomicBoolean(true);
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      String msgPath = PropertyPathConfig.getPath(PropertyType.MESSAGES, clusterName, instanceName);

      _zkclient.subscribeChildChanges(msgPath, new IZkChildListener() {

        @Override
        public void handleChildChange(String parentPath, List<String> currentChilds)
            throws Exception {
          if (currentChilds != null && currentChilds.size() > 1) {
            List<ZNRecord> records =
                accessor.getBaseDataAccessor().getChildren(parentPath, null, 0);
            int transitionMsgCount = 0;
            for (ZNRecord record : records) {
              Message msg = new Message(record);
              if (msg.getMsgType().equals(Message.MessageType.STATE_TRANSITION.toString())) {
                transitionMsgCount++;
              }
            }

            if (transitionMsgCount > 1) {
              success.set(false);
              Assert.fail("Should not see more than 1 message");
            }
          }

        }
      });
    }

    MockController controller =
        new MockController(_zkaddr, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(_zkaddr, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, clusterName));
    Assert.assertTrue(result);

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            clusterName));
    Assert.assertTrue(result);

    Assert.assertTrue(success.get());

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }
}
