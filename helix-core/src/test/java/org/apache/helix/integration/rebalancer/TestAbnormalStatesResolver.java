package org.apache.helix.integration.rebalancer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.Criteria;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.constraint.ExcessiveTopStateResolver;
import org.apache.helix.controller.rebalancer.constraint.MockAbnormalStateResolver;
import org.apache.helix.controller.rebalancer.constraint.MonitoredAbnormalResolver;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestAbnormalStatesResolver extends ZkStandAloneCMTestBase {
  // TODO: remove this wait time once we have a better way to determine if the rebalance has been
  // TODO: done as a reaction of the test operations.
  protected static final int DEFAULT_REBALANCE_PROCESSING_WAIT_TIME = 1000;

  @Test
  public void testConfigureResolver() {
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER_NAME);
    // Verify the initial setup.
    cache.refresh(_controller.getHelixDataAccessor());
    for (String stateModelDefName : cache.getStateModelDefMap().keySet()) {
      Assert.assertEquals(cache.getAbnormalStateResolver(stateModelDefName).getResolverClass(),
          MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER.getResolverClass());
    }

    // Update the resolver configuration for MasterSlave state model.
    ConfigAccessor configAccessor = new ConfigAccessor.Builder().setZkAddress(ZK_ADDR).build();
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(
        ImmutableMap.of(MasterSlaveSMD.name, MockAbnormalStateResolver.class.getName()));
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    cache.requireFullRefresh();
    cache.refresh(_controller.getHelixDataAccessor());
    for (String stateModelDefName : cache.getStateModelDefMap().keySet()) {
      Assert.assertEquals(cache.getAbnormalStateResolver(stateModelDefName).getResolverClass(),
          stateModelDefName.equals(MasterSlaveSMD.name) ?
              MockAbnormalStateResolver.class :
              MonitoredAbnormalResolver.DUMMY_STATE_RESOLVER.getResolverClass());
    }

    // Reset the resolver map
    clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(Collections.emptyMap());
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  @Test(dependsOnMethods = "testConfigureResolver")
  public void testExcessiveTopStateResolver() throws InterruptedException {
    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(verifier.verify());

    // 1. Find a partition with a MASTER replica and a SLAVE replica
    HelixAdmin admin = new ZKHelixAdmin.Builder().setZkAddress(ZK_ADDR).build();
    ExternalView ev = admin.getResourceExternalView(CLUSTER_NAME, TEST_DB);
    String targetPartition = ev.getPartitionSet().iterator().next();
    Map<String, String> partitionAssignment = ev.getStateMap(targetPartition);
    String slaveHost = partitionAssignment.entrySet().stream()
        .filter(entry -> entry.getValue().equals(MasterSlaveSMD.States.SLAVE.name())).findAny()
        .get().getKey();
    long previousMasterUpdateTime =
        getTopStateUpdateTime(ev, targetPartition, MasterSlaveSMD.States.MASTER.name());

    // Build SLAVE to MASTER message
    String msgId = new UUID(123, 456).toString();
    Message msg = createMessage(Message.MessageType.STATE_TRANSITION, msgId,
        MasterSlaveSMD.States.SLAVE.name(), MasterSlaveSMD.States.MASTER.name(), TEST_DB,
        slaveHost);
    msg.setStateModelDef(MasterSlaveSMD.name);

    Criteria cr = new Criteria();
    cr.setInstanceName(slaveHost);
    cr.setRecipientInstanceType(InstanceType.PARTICIPANT);
    cr.setSessionSpecific(true);
    cr.setPartition(targetPartition);
    cr.setResource(TEST_DB);
    cr.setClusterName(CLUSTER_NAME);

    AsyncCallback callback = new AsyncCallback() {
      @Override
      public void onTimeOut() {
        Assert.fail("The test state transition timeout.");
      }

      @Override
      public void onReplyMessage(Message message) {
        Assert.assertEquals(message.getMsgState(), Message.MessageState.READ);
      }
    };

    // 2. Send the SLAVE to MASTER message to the SLAVE host to make abnormal partition states.

    // 2.A. Without resolver, the fixing is not completely done by the default rebalancer logic.
    _controller.getMessagingService()
        .sendAndWait(cr, msg, callback, (int) TestHelper.WAIT_DURATION);
    // Wait until the partition status is fixed, verify if the result is as expected
    verifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
            .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
            .build();
    Assert.assertTrue(verifier.verifyByPolling());
    ev = admin.getResourceExternalView(CLUSTER_NAME, TEST_DB);
    Assert.assertEquals(ev.getStateMap(targetPartition).values().stream()
        .filter(state -> state.equals(MasterSlaveSMD.States.MASTER.name())).count(), 1);
    // Since the resolver is not used in the auto default fix process, there is no update on the
    // original master. So if there is any data issue, it was not fixed.
    long currentMasterUpdateTime =
        getTopStateUpdateTime(ev, targetPartition, MasterSlaveSMD.States.MASTER.name());
    Assert.assertFalse(currentMasterUpdateTime > previousMasterUpdateTime);

    // 2.B. with resolver configured, the fixing is complete.
    ConfigAccessor configAccessor = new ConfigAccessor.Builder().setZkAddress(ZK_ADDR).build();
    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(
        ImmutableMap.of(MasterSlaveSMD.name, ExcessiveTopStateResolver.class.getName()));
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    _controller.getMessagingService()
        .sendAndWait(cr, msg, callback, (int) TestHelper.WAIT_DURATION);
    // Wait until the partition status is fixed, verify if the result is as expected
    Assert.assertTrue(verifier.verifyByPolling());
    ev = admin.getResourceExternalView(CLUSTER_NAME, TEST_DB);
    Assert.assertEquals(ev.getStateMap(targetPartition).values().stream()
        .filter(state -> state.equals(MasterSlaveSMD.States.MASTER.name())).count(), 1);
    // Now the resolver is used in the auto fix process, the original master has also been refreshed.
    // The potential data issue has been fixed in this process.
    currentMasterUpdateTime =
        getTopStateUpdateTime(ev, targetPartition, MasterSlaveSMD.States.MASTER.name());
    Assert.assertTrue(currentMasterUpdateTime > previousMasterUpdateTime);

    // Reset the resolver map
    clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.setAbnormalStateResolverMap(Collections.emptyMap());
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
  }

  private long getTopStateUpdateTime(ExternalView ev, String partition, String state) {
    String topStateHost = ev.getStateMap(partition).entrySet().stream()
        .filter(entry -> entry.getValue().equals(state)).findFirst().get().getKey();
    MockParticipantManager participant = Arrays.stream(_participants)
        .filter(instance -> instance.getInstanceName().equals(topStateHost)).findFirst().get();

    HelixDataAccessor accessor = _controller.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    CurrentState currentState = accessor.getProperty(keyBuilder
        .currentState(participant.getInstanceName(), participant.getSessionId(),
            ev.getResourceName()));
    return currentState.getEndTime(partition);
  }
}
