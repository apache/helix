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
import java.util.Date;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.helix.BaseDataAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConstraints.ConstraintAttribute;
import org.apache.helix.model.ClusterConstraints.ConstraintType;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.model.builder.ConstraintItemBuilder;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPartitionLevelTransitionConstraint extends ZkTestBase {

  private static Logger LOG = LoggerFactory.getLogger(TestPartitionLevelTransitionConstraint.class);

  final Queue<Message> _msgOrderList = new ConcurrentLinkedQueue<Message>();

  public class BootstrapStateModel extends StateModel {
    public void onBecomeBootstrapFromOffline(Message message, NotificationContext context) {
      LOG.info("Become Bootstrap from Offline");
      _msgOrderList.add(message);
    }

    public void onBecomeOfflineFromBootstrap(Message message, NotificationContext context) {
      LOG.info("Become Offline from Bootstrap");
      _msgOrderList.add(message);
    }

    public void onBecomeSlaveFromBootstrap(Message message, NotificationContext context) {
      LOG.info("Become Slave from Bootstrap");
      _msgOrderList.add(message);
    }

    public void onBecomeBootstrapFromSlave(Message message, NotificationContext context) {
      LOG.info("Become  Bootstrap from Slave");
      _msgOrderList.add(message);
    }

    public void onBecomeMasterFromSlave(Message message, NotificationContext context) {
      LOG.info("Become Master from Slave");
      _msgOrderList.add(message);
    }

    public void onBecomeSlaveFromMaster(Message message, NotificationContext context) {
      LOG.info("Become Slave from Master");
      _msgOrderList.add(message);
    }

  }

  public class BootstrapStateModelFactory extends StateModelFactory<BootstrapStateModel> {

    @Override
    public BootstrapStateModel createNewStateModel(String resource, String stateUnitKey) {
      BootstrapStateModel model = new BootstrapStateModel();
      return model;
    }
  }

  @Test
  public void test() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;
    int n = 2;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        1, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", false); // do not rebalance

    // setup semi-auto ideal-state
    BaseDataAccessor<ZNRecord> baseAccessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    HelixDataAccessor accessor = new ZKHelixDataAccessor(clusterName, baseAccessor);
    StateModelDefinition stateModelDef = defineStateModel();
    accessor.setProperty(accessor.keyBuilder().stateModelDef("Bootstrap"), stateModelDef);
    IdealState idealState = accessor.getProperty(accessor.keyBuilder().idealStates("TestDB0"));
    idealState.setStateModelDefRef("Bootstrap");
    idealState.setReplicas("2");
    idealState.getRecord().setListField("TestDB0_0",
        Arrays.asList("localhost_12919", "localhost_12918"));
    accessor.setProperty(accessor.keyBuilder().idealStates("TestDB0"), idealState);

    // setup partition-level constraint
    ConstraintItemBuilder constraintItemBuilder = new ConstraintItemBuilder();

    constraintItemBuilder
        .addConstraintAttribute(ConstraintAttribute.MESSAGE_TYPE.toString(), "STATE_TRANSITION")
        .addConstraintAttribute(ConstraintAttribute.PARTITION.toString(), ".*")
        .addConstraintAttribute(ConstraintAttribute.CONSTRAINT_VALUE.toString(), "1");

    HelixAdmin admin = new ZKHelixAdmin(_gZkClient);
    admin.setConstraint(clusterName, ConstraintType.MESSAGE_CONSTRAINT, "constraint1",
        constraintItemBuilder.build());

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller");
    controller.syncStart();

    // start 1st participant
    MockParticipantManager[] participants = new MockParticipantManager[n];
    String instanceName1 = "localhost_12918";

    participants[0] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName1);
    participants[0].getStateMachineEngine().registerStateModelFactory("Bootstrap",
        new BootstrapStateModelFactory());
    participants[0].syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // start 2nd participant which will be the master for Test0_0
    String instanceName2 = "localhost_12919";
    participants[1] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName2);
    participants[1].getStateMachineEngine().registerStateModelFactory("Bootstrap",
        new BootstrapStateModelFactory());
    participants[1].syncStart();

    result =
        ClusterStateVerifier
            .verifyByZkCallback(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR,
                clusterName));
    Assert.assertTrue(result);

    // check we received the message in the right order
    Assert.assertEquals(_msgOrderList.size(), 7);

    Message[] _msgOrderArray = _msgOrderList.toArray(new Message[0]);
    assertMessage(_msgOrderArray[0], "OFFLINE", "BOOTSTRAP", instanceName1);
    assertMessage(_msgOrderArray[1], "BOOTSTRAP", "SLAVE", instanceName1);
    assertMessage(_msgOrderArray[2], "SLAVE", "MASTER", instanceName1);

    // after we start the 2nd instance, the messages should be received in the following order:
    // 1) offline->bootstrap for localhost_12919
    // 2) bootstrap->slave for localhost_12919
    // 3) master->slave for localhost_12918
    // 4) slave->master for localhost_12919
    assertMessage(_msgOrderArray[3], "OFFLINE", "BOOTSTRAP", instanceName2);
    assertMessage(_msgOrderArray[4], "BOOTSTRAP", "SLAVE", instanceName2);
    assertMessage(_msgOrderArray[5], "MASTER", "SLAVE", instanceName1);
    assertMessage(_msgOrderArray[6], "SLAVE", "MASTER", instanceName2);

    // clean up
    // wait for all zk callbacks done
    controller.syncStop();
    for (int i = 0; i < n; i++) {
      participants[i].syncStop();
    }
    deleteCluster(clusterName);

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private static void assertMessage(Message msg, String fromState, String toState, String instance) {
    Assert.assertEquals(msg.getFromState(), fromState);
    Assert.assertEquals(msg.getToState(), toState);
    Assert.assertEquals(msg.getTgtName(), instance);
  }

  private static StateModelDefinition defineStateModel() {
    StateModelDefinition.Builder builder = new StateModelDefinition.Builder("Bootstrap");
    // Add states and their rank to indicate priority. Lower the rank higher the priority
    builder.addState("MASTER", 1);
    builder.addState("SLAVE", 2);
    builder.addState("BOOTSTRAP", 3);
    builder.addState("OFFLINE");
    builder.addState("DROPPED");
    // Set the initial state when the node starts
    builder.initialState("OFFLINE");

    // Add transitions between the states.
    builder.addTransition("OFFLINE", "BOOTSTRAP", 3);
    builder.addTransition("BOOTSTRAP", "SLAVE", 2);
    builder.addTransition("SLAVE", "MASTER", 1);
    builder.addTransition("MASTER", "SLAVE", 4);
    builder.addTransition("SLAVE", "OFFLINE", 5);
    builder.addTransition("OFFLINE", "DROPPED", 6);

    // set constraints on states.
    // static constraint
    builder.upperBound("MASTER", 1);
    // dynamic constraint, R means it should be derived based on the replication factor.
    builder.dynamicUpperBound("SLAVE", "R");

    StateModelDefinition statemodelDefinition = builder.build();

    assert (statemodelDefinition.isValid());

    return statemodelDefinition;
  }
}
