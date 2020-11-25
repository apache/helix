package org.apache.helix.controller.stages;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPerReplicaThrottleStage extends BaseStageTest {

  private void preSetup(String[] resources, int nPartition, int nReplica) {
    setupIdealState(nReplica, resources, nPartition, nReplica,
        IdealState.RebalanceMode.FULL_AUTO, "MasterSlave", null, null, 2);
    setupStateModel();
    setupLiveInstances(nReplica);
  }

  private void setupThrottleConfig(int recoveryLimit, int loadLimit) {
    ClusterConfig clusterConfig = accessor.getProperty(accessor.keyBuilder().clusterConfig());
    clusterConfig.setStateTransitionThrottleConfigs(ImmutableList
        .of(new StateTransitionThrottleConfig(
                StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE,
                StateTransitionThrottleConfig.ThrottleScope.INSTANCE, recoveryLimit),
            new StateTransitionThrottleConfig(
                StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE,
                StateTransitionThrottleConfig.ThrottleScope.INSTANCE, loadLimit)));
    setClusterConfig(clusterConfig);
  }

  // case 1. N1(M), N2(S), N3(O), message N3(O->S) is treated as load
  @Test
  public void testUncessaryForMinReplicasAsLoad() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);
    setupThrottleConfig(3, 0);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        Message msg = new Message(Message.MessageType.STATE_TRANSITION, "001");
        msg.setToState("SLAVE");
        msg.setFromState("OFFLINE");
        msg.setTgtName(HOSTNAME_PREFIX + 2);
        messageOutput.addMessage(resource, partition, msg);
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "SLAVE");
        List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
        bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> perReplicaThottledLoad = event.getAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name());
    Assert.assertTrue(perReplicaThottledLoad.size() == 1);
    Message msg = perReplicaThottledLoad.get(0);
    Assert.assertTrue(msg.getId().equals("001"));
  }

  // case 2. N1(S), N2(S), N3(O), message N1(S->M) is treated as recovery
  @Test
  public void testRecovery() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);
    setupThrottleConfig(0, 1);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "SLAVE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        Message msg = new Message(Message.MessageType.STATE_TRANSITION, "001");
        msg.setToState("MASTER");
        msg.setFromState("SLAVE");
        msg.setTgtName(HOSTNAME_PREFIX + 0);
        messageOutput.addMessage(resource, partition, msg);
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
        bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> msgs = event.getAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 1);
    Message msg = msgs.get(0);
    Assert.assertTrue(msg.getId().equals("001"));
  }

  // Case 3, partition bootup, only two counted as recovery, one as load
  @Test
  public void testBootupRecoveryAndLoad() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);
    setupThrottleConfig(0, 0);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "OFFLINE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "OFFLINE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        Message msg = new Message(Message.MessageType.STATE_TRANSITION, "001");
        msg.setToState("SLAVE");
        msg.setFromState("OFFLINE");
        msg.setTgtName(HOSTNAME_PREFIX + 0);
        messageOutput.addMessage(resource, partition, msg);
        msg = new Message(Message.MessageType.STATE_TRANSITION, "002");
        msg.setToState("SLAVE");
        msg.setFromState("OFFLINE");
        msg.setTgtName(HOSTNAME_PREFIX + 1);
        messageOutput.addMessage(resource, partition, msg);
        msg = new Message(Message.MessageType.STATE_TRANSITION, "003");
        msg.setToState("SLAVE");
        msg.setFromState("OFFLINE");
        msg.setTgtName(HOSTNAME_PREFIX + 2);
        messageOutput.addMessage(resource, partition, msg);
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "SLAVE");
        List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
        bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
      }
    }

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> msgs = event.getAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 2);
    msgs = event.getAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 1);
  }

  // Case 4, Downward only throttling due to error partitions
  @Test
  public void testDownwardOnlyThrottle() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 3;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }
    preSetup(resources, nPartition, nReplica);
    setupThrottleConfig(0, 2);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        if (p == 0) {
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "SLAVE");
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "MASTER");
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
          // this upward message should be throttled.
          Message msgUp = new Message(Message.MessageType.STATE_TRANSITION, "001");
          msgUp.setToState("SLAVE");
          msgUp.setFromState("OFFLINE");
          msgUp.setTgtName(HOSTNAME_PREFIX + 2);
          // this downward message should not be throttled.
          Message msgDown = new Message(Message.MessageType.STATE_TRANSITION, "002");
          msgDown.setToState("SLAVE");
          msgDown.setFromState("MASTER");
          msgDown.setTgtName(HOSTNAME_PREFIX + 1);
          messageOutput.addMessage(resource, partition, msgUp);
          messageOutput.addMessage(resource, partition, msgDown);
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "SLAVE");
          List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
          bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
        } else {
          // setup error partition
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
          currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "ERROR");
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
          bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "SLAVE");
          List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
          bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
        }
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> msgs = event.getAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 1);
    Message msg = msgs.get(0);
    Assert.assertTrue(msg.getId().equals("001"));

    MessageOutput msgsOut = event.getAttribute(AttributeName.PER_REPLICA_THROTTLED_MESSAGES.name());
    msgs = msgsOut.getMessages(resources[0], new Partition(resources[0] + "_" + 0));
    Assert.assertTrue(msgs.size() == 1);
    msg = msgs.get(0);
    Assert.assertTrue(msg.getId().equals("002"));
  }

  // Case 5, Disabled instance not limited by throttling
  @Test
  public void testDisabledPartitionNotThrottled() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);
    setupInstances(nReplica);
    setupThrottleConfig(0, 0);

    // disable partition on instances
    for (int i = 0; i < nReplica; i++ ) {
      String instanceName = HOSTNAME_PREFIX + i;
      PropertyKey key = accessor.keyBuilder().instanceConfig(instanceName);
      InstanceConfig instanceConfig = accessor.getProperty(key);
      instanceConfig.setInstanceEnabledForPartition(resources[0], resources[0] + "_" + 0, false);
      accessor.setProperty(key, instanceConfig);
    }

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        Message msg = new Message(Message.MessageType.STATE_TRANSITION, "001");
        msg.setToState("SLAVE");
        msg.setFromState("MASTER");
        msg.setTgtName(HOSTNAME_PREFIX + 0);
        messageOutput.addMessage(resource, partition, msg);
        msg = new Message(Message.MessageType.STATE_TRANSITION, "002");
        msg.setToState("OFFLINE");
        msg.setFromState("SLAVE");
        msg.setTgtName(HOSTNAME_PREFIX + 1);
        messageOutput.addMessage(resource, partition, msg);
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "OFFLINE");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "OFFLINE");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
        bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> msgs = event.getAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 0);
    msgs = event.getAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 0);
  }

  // Case 6, Drop message not limited by throttling
  @Test
  public void testDropMessageNoteThrottled() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);
    setupThrottleConfig(0, 0);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));
    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 1, "SLAVE");
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 2, "OFFLINE");
        Message msg = new Message(Message.MessageType.STATE_TRANSITION, "001");
        msg.setToState("ERROR");
        msg.setFromState("SLAVE");
        msg.setTgtName(HOSTNAME_PREFIX + 1);
        messageOutput.addMessage(resource, partition, msg);
        msg = new Message(Message.MessageType.STATE_TRANSITION, "002");
        msg.setToState("DROPPED");
        msg.setFromState("OFFLINE");
        msg.setTgtName(HOSTNAME_PREFIX + 2);
        messageOutput.addMessage(resource, partition, msg);
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 0, "MASTER");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 1, "ERROR");
        bestPossibleStateOutput.setState(resource, partition, HOSTNAME_PREFIX + 2, "DROPPED");
        List<String> list = Arrays.asList(HOSTNAME_PREFIX + 0, HOSTNAME_PREFIX + 1, HOSTNAME_PREFIX + 2);
        bestPossibleStateOutput.setPreferenceList(resource, partition.getPartitionName(), list);
      }
    }
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.MESSAGES_SELECTED.name(), messageOutput);
    event.addAttribute(AttributeName.BEST_POSSIBLE_STATE.name(), bestPossibleStateOutput);
    event.addAttribute(AttributeName.ControllerDataProvider.name(),
        new ResourceControllerDataProvider());

    runStage(event, new ReadClusterDataStage());
    runStage(event, new PerReplicaThrottleStage(true));

    List<Message> msgs = event.getAttribute(AttributeName.PER_REPLICA_THROTTLED_RECOVERY_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 0);
    msgs = event.getAttribute(AttributeName.PER_REPLICA_THOTTLED_LOAD_MESSAGES.name());
    Assert.assertTrue(msgs.size() == 0);
  }

  // Case 7, Trigger into maintainence mode
  @Test
  public void testTriggerMaintenanceMode() {
    // TODO
  }

  protected Map<String, Resource> getResourceMap(String[] resources, int partitions,
      String stateModel) {
    Map<String, Resource> resourceMap = new HashMap<String, Resource>();

    for (String r : resources) {
      Resource testResource = new Resource(r);
      testResource.setStateModelDefRef(stateModel);
      for (int i = 0; i < partitions; i++) {
        testResource.addPartition(r + "_" + i);
      }
      resourceMap.put(r, testResource);
    }

    return resourceMap;
  }
}
