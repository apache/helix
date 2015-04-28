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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.I0Itec.zkclient.ZkServer;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.TestHelper;
import org.apache.helix.api.State;
import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.api.id.StateModelFactoryId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.testutil.ZkTestUtil;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkReconnect {
  private static final Logger LOG = Logger.getLogger(TestZkReconnect.class);

  @Test
  public void testZKReconnect() throws Exception {
    final AtomicReference<ZkServer> zkServerRef = new AtomicReference<ZkServer>();
    final int zkPort = ZkTestUtil.availableTcpPort();
    final String zkAddr = String.format("localhost:%d", zkPort);
    ZkServer zkServer = TestHelper.startZkServer(zkAddr);
    zkServerRef.set(zkServer);

    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    // Setup cluster
    LOG.info("Setup clusters");
    ClusterSetup clusterSetup = new ClusterSetup(zkAddr);
    clusterSetup.addCluster(clusterName, true);

    // Registers and starts controller
    LOG.info("Starts controller");
    HelixManager controller =
        HelixManagerFactory.getZKHelixManager(clusterName, null, InstanceType.CONTROLLER, zkAddr);
    controller.connect();

    // Registers and starts participant
    LOG.info("Starts participant");
    String hostname = "localhost";
    String instanceId = String.format("%s_%d", hostname, 1);
    clusterSetup.addInstanceToCluster(clusterName, instanceId);
    HelixManager participant =
        HelixManagerFactory.getZKHelixManager(clusterName, instanceId, InstanceType.PARTICIPANT,
            zkAddr);
    participant.connect();

    LOG.info("Register state machine");
    final CountDownLatch latch = new CountDownLatch(1);
    participant.getStateMachineEngine().registerStateModelFactory(
        StateModelDefId.OnlineOffline, "test", new StateTransitionHandlerFactory<TransitionHandler>() {
          @Override
          public TransitionHandler createStateTransitionHandler(ResourceId resource, PartitionId stateUnitKey) {
            return new SimpleStateModel(latch);
          }
        });

    String resourceName = "test-resource";
    LOG.info("Ideal state assignment");
    HelixAdmin helixAdmin = participant.getClusterManagmentTool();
    helixAdmin.addResource(clusterName, resourceName, 1, "OnlineOffline",
        IdealState.RebalanceMode.CUSTOMIZED.toString());

    IdealState idealState = helixAdmin.getResourceIdealState(clusterName, resourceName);
    idealState.setReplicas("1");
    idealState.setStateModelFactoryId(StateModelFactoryId.from("test"));
    idealState.setPartitionState(PartitionId.from(ResourceId.from(resourceName), "0"),
        ParticipantId.from(instanceId), State.from("ONLINE"));

    LOG.info("Shutdown ZK server");
    TestHelper.stopZkServer(zkServerRef.get());
    Executors.newSingleThreadScheduledExecutor().schedule(new Runnable() {

      @Override
      public void run() {
        try {
          LOG.info("Restart ZK server");
          // zkServer.set(TestUtils.startZookeeper(zkDir, zkPort));
          zkServerRef.set(TestHelper.startZkServer(zkAddr, null, false));
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }, 2L, TimeUnit.SECONDS);

    // future.get();

    LOG.info("Before update ideal state");
    helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
    LOG.info("After update ideal state");

    LOG.info("Wait for OFFLINE->ONLINE state transition");
    try {
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      // wait until stable state
      boolean result =
          ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(zkAddr,
              clusterName));
      Assert.assertTrue(result);

    } finally {
      participant.disconnect();
      zkServerRef.get().shutdown();
    }
  }

  public static final class SimpleStateModel extends TransitionHandler {

    private final CountDownLatch latch;

    public SimpleStateModel(CountDownLatch latch) {
      this.latch = latch;
    }

    public void onBecomeOnlineFromOffline(Message message, NotificationContext context) {
      // LOG.info(HelixUtils.toString(message));
      LOG.info("message: " + message);
      latch.countDown();
    }
  }

}
