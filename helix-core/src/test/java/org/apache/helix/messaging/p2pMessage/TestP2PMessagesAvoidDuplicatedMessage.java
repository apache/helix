package org.apache.helix.messaging.p2pMessage;

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
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.helix.HelixConstants;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.resource.ResourceMessageGenerationPhase;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestP2PMessagesAvoidDuplicatedMessage extends BaseStageTest {
  String _db = "testDB";
  int _numPartition = 1;
  int _numReplica = 3;

  Partition _partition = new Partition(_db + "_0");

  ResourceControllerDataProvider _dataCache;
  Pipeline _fullPipeline;
  Pipeline _messagePipeline;

  ResourcesStateMap _bestpossibleState;

  private void preSetup() throws Exception {
    setupIdealState(3, new String[] { _db }, _numPartition, _numReplica,
        IdealState.RebalanceMode.SEMI_AUTO, BuiltInStateModelDefinitions.MasterSlave.name());
    setupStateModel();
    setupInstances(3);
    setupLiveInstances(3);

    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.enableP2PMessage(true);
    setClusterConfig(clusterConfig);

    Map<String, Resource> resourceMap = getResourceMap(new String[] { _db }, _numPartition,
        BuiltInStateModelDefinitions.MasterSlave.name(), clusterConfig, null);

    _dataCache = new ResourceControllerDataProvider();
    _dataCache.setAsyncTasksThreadPool(Executors.newSingleThreadExecutor());

    event.addAttribute(AttributeName.ControllerDataProvider.name(), _dataCache);
    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), new CurrentStateOutput());
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    _fullPipeline = new Pipeline("FullPipeline");
    _fullPipeline.addStage(new ReadClusterDataStage());
    _fullPipeline.addStage(new BestPossibleStateCalcStage());
    _fullPipeline.addStage(new IntermediateStateCalcStage());
    _fullPipeline.addStage(new ResourceMessageGenerationPhase());
    _fullPipeline.addStage(new MessageSelectionStage());
    _fullPipeline.addStage(new MessageThrottleStage());

    _messagePipeline = new Pipeline("MessagePipeline");
    _messagePipeline.addStage(new ResourceMessageGenerationPhase());
    _messagePipeline.addStage(new MessageSelectionStage());
    _messagePipeline.addStage(new MessageThrottleStage());


    _fullPipeline.handle(event);
    _bestpossibleState =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
  }


  @Test
  public void testP2PAvoidDuplicatedMessage() throws Exception {
    preSetup();

    // Scenario 1:
    // Disable old master ((initialMaster) instance,
    // Validate: a M->S message should be sent to initialMaster with a P2P message attached for secondMaster.
    String initialMaster = getTopStateInstance(_bestpossibleState.getInstanceStateMap(_db, _partition),
        MasterSlaveSMD.States.MASTER.name());
    Assert.assertNotNull(initialMaster);

    // disable existing master instance
    admin.enableInstance(_clusterName, initialMaster, false);
    _dataCache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    _dataCache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);

    CurrentStateOutput currentStateOutput =
        populateCurrentStateFromBestPossible(_bestpossibleState);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    _fullPipeline.handle(event);

    _bestpossibleState = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    MessageOutput messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages = messageOutput.getMessages(_db, _partition);

    Assert.assertEquals(messages.size(), 1);
    Message toSlaveMessage = messages.get(0);
    Assert.assertEquals(toSlaveMessage.getTgtName(), initialMaster);
    Assert.assertEquals(toSlaveMessage.getFromState(), MasterSlaveSMD.States.MASTER.name());
    Assert.assertEquals(toSlaveMessage.getToState(), MasterSlaveSMD.States.SLAVE.name());

    // verify p2p message are attached to the M->S message sent to the old master instance
    Assert.assertEquals(toSlaveMessage.getRelayMessages().entrySet().size(), 1);
    String secondMaster =
        getTopStateInstance(_bestpossibleState.getInstanceStateMap(_db, _partition), MasterSlaveSMD.States.MASTER.name());

    Message relayMessage = toSlaveMessage.getRelayMessage(secondMaster);
    Assert.assertNotNull(relayMessage);
    Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
    Assert.assertEquals(relayMessage.getTgtName(), secondMaster);
    Assert.assertEquals(relayMessage.getRelaySrcHost(), initialMaster);
    Assert.assertEquals(relayMessage.getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(relayMessage.getToState(), MasterSlaveSMD.States.MASTER.name());

    // Scenario 2A:
    // Old master (initialMaster) completes the M->S transition,
    // but has not forward p2p message to new master (secondMaster) yet.
    // Validate: Controller should not send S->M message to new master.
    currentStateOutput.setCurrentState(_db, _partition, initialMaster, "SLAVE");
    currentStateOutput.setPendingMessage(_db, _partition, initialMaster, toSlaveMessage);
    currentStateOutput.setPendingRelayMessage(_db, _partition, initialMaster, relayMessage);

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    _fullPipeline.handle(event);

    messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 0);


    // Scenario 2B:
    // Old master (initialMaster) completes the M->S transition,
    // There is a pending p2p message to new master (secondMaster).
    // Validate: Controller should send S->M message to new master at same time.

    currentStateOutput.setCurrentState(_db, _partition, initialMaster, "SLAVE");
    currentStateOutput.getPendingMessageMap(_db, _partition).clear();
    currentStateOutput.setPendingRelayMessage(_db, _partition, initialMaster, relayMessage);

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    _messagePipeline.handle(event);

    messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 2);

    boolean hasToOffline = false;
    boolean hasToMaster = false;
    for (Message msg : messages) {
      if (msg.getToState().equals(MasterSlaveSMD.States.MASTER.name()) && msg.getTgtName()
          .equals(secondMaster)) {
        hasToMaster = true;
      }
      if (msg.getToState().equals(MasterSlaveSMD.States.OFFLINE.name()) && msg.getTgtName()
          .equals(initialMaster)) {
        hasToOffline = true;
      }
    }
    Assert.assertTrue(hasToMaster);
    Assert.assertTrue(hasToOffline);

    // Secenario 2C
    // Old master (initialMaster) completes the M->S transition,
    // There is a pending p2p message to new master (secondMaster).
    // However, the new master has been changed in bestPossible
    // Validate: Controller should not send S->M message to the third master at same time.

    String thirdMaster =
        getTopStateInstance(_bestpossibleState.getInstanceStateMap(_db, _partition),
            MasterSlaveSMD.States.SLAVE.name());

    Map<String, String> instanceStateMap = _bestpossibleState.getInstanceStateMap(_db, _partition);
    instanceStateMap.put(secondMaster, "SLAVE");
    instanceStateMap.put(thirdMaster, "MASTER");
    _bestpossibleState.setState(_db, _partition, instanceStateMap);


    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);
    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), _bestpossibleState);

    _messagePipeline.handle(event);

    messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 1);
    Assert.assertTrue(messages.get(0).getToState().equals("OFFLINE"));
    Assert.assertTrue(messages.get(0).getTgtName().equals(initialMaster));


    // Scenario 3:
    // Old master (initialMaster) completes the M->S transition,
    // and has already forwarded p2p message to new master (secondMaster)
    // The original S->M message sent to old master has been removed.
    // Validate: Controller should send S->O to old master, but not S->M message to new master.
    instanceStateMap = _bestpossibleState.getInstanceStateMap(_db, _partition);
    instanceStateMap.put(secondMaster, "MASTER");
    instanceStateMap.put(thirdMaster, "SLAVE");
    _bestpossibleState.setState(_db, _partition, instanceStateMap);

    currentStateOutput =
        populateCurrentStateFromBestPossible(_bestpossibleState);
    currentStateOutput.setCurrentState(_db, _partition, initialMaster, "SLAVE");
    currentStateOutput.setPendingMessage(_db, _partition, secondMaster, relayMessage);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    _fullPipeline.handle(event);

    messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 1);

    Message toOfflineMessage = messages.get(0);
    Assert.assertEquals(toOfflineMessage.getTgtName(), initialMaster);
    Assert.assertEquals(toOfflineMessage.getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(toOfflineMessage.getToState(), MasterSlaveSMD.States.OFFLINE.name());


    // Scenario 4:
    // The old master (initialMaster) finish state transition, but has not forward p2p message yet.
    // Then the preference list has changed, so now the new master (thirdMaster) is different from previously calculated new master (secondMaster)
    // Validate: controller should not send S->M to thirdMaster.
    currentStateOutput.setCurrentState(_db, _partition, initialMaster, "OFFLINE");
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    thirdMaster =
        getTopStateInstance(_bestpossibleState.getInstanceStateMap(_db, _partition),
            MasterSlaveSMD.States.SLAVE.name());

    instanceStateMap = _bestpossibleState.getInstanceStateMap(_db, _partition);
    instanceStateMap.put(secondMaster, "SLAVE");
    instanceStateMap.put(thirdMaster, "MASTER");
    _bestpossibleState.setState(_db, _partition, instanceStateMap);

    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), _bestpossibleState);

    _messagePipeline.handle(event);

    messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 0);


    // Scenario 5:
    // The initial master has forwarded the p2p message to secondMaster and deleted original M->S message on initialMaster,
    // But the S->M state-transition has not completed yet in secondMaster.
    // Validate: Controller should not send S->M to thirdMaster.
    currentStateOutput.setPendingMessage(_db, _partition, secondMaster, relayMessage);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    event.addAttribute(AttributeName.INTERMEDIATE_STATE.name(), _bestpossibleState);

    _messagePipeline.handle(event);

    messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 0);


    // Scenario 5:
    // The thirdMaster completed the state transition and deleted the p2p message.
    // Validate: Controller should M->S message to secondMaster.
    currentStateOutput =
        populateCurrentStateFromBestPossible(_bestpossibleState);
    currentStateOutput.setCurrentState(_db, _partition, secondMaster, "MASTER");
    currentStateOutput.setCurrentState(_db, _partition, thirdMaster, "SLAVE");

    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    _messagePipeline.handle(event);

    messageOutput =
        event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    messages = messageOutput.getMessages(_db, _partition);
    Assert.assertEquals(messages.size(), 1);

    toSlaveMessage = messages.get(0);
    Assert.assertEquals(toSlaveMessage.getTgtName(), secondMaster);
    Assert.assertEquals(toSlaveMessage.getFromState(), MasterSlaveSMD.States.MASTER.name());
    Assert.assertEquals(toSlaveMessage.getToState(), MasterSlaveSMD.States.SLAVE.name());

    // verify p2p message are attached to the M->S message sent to the secondMaster
    Assert.assertEquals(toSlaveMessage.getRelayMessages().entrySet().size(), 1);

    relayMessage = toSlaveMessage.getRelayMessage(thirdMaster);
    Assert.assertNotNull(relayMessage);
    Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
    Assert.assertEquals(relayMessage.getTgtName(), thirdMaster);
    Assert.assertEquals(relayMessage.getRelaySrcHost(), secondMaster);
    Assert.assertEquals(relayMessage.getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(relayMessage.getToState(), MasterSlaveSMD.States.MASTER.name());
  }


  private String getTopStateInstance(Map<String, String> instanceStateMap, String topState) {
    String masterInstance = null;
    for (Map.Entry<String, String> e : instanceStateMap.entrySet()) {
      if (topState.equals(e.getValue())) {
        masterInstance = e.getKey();
      }
    }

    return masterInstance;
  }

  private CurrentStateOutput populateCurrentStateFromBestPossible(
      ResourcesStateMap bestPossibleStateOutput) {
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    for (String resource : bestPossibleStateOutput.getResourceStatesMap().keySet()) {
      PartitionStateMap partitionStateMap = bestPossibleStateOutput.getPartitionStateMap(resource);
      for (Partition p : partitionStateMap.partitionSet()) {
        Map<String, String> stateMap = partitionStateMap.getPartitionMap(p);

        for (Map.Entry<String, String> e : stateMap.entrySet()) {
          currentStateOutput.setCurrentState(resource, p, e.getKey(), e.getValue());
        }
      }
    }
    return currentStateOutput;
  }
}
