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

import java.lang.reflect.Method;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.common.ResourcesStateMap;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.controller.stages.resource.ResourceMessageDispatchStage;
import org.apache.helix.controller.stages.resource.ResourceMessageGenerationPhase;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TestP2PMessages extends BaseStageTest {
  private String _db = "testDB";
  private int _numPartition = 1;
  private int _numReplica = 3;

  private Partition _partition = new Partition(_db + "_0");

  private ResourceControllerDataProvider _dataCache;
  private Pipeline _fullPipeline;

  private ResourcesStateMap _initialStateMap;

  private Set<String> _instances;
  private Map<String, LiveInstance> _liveInstanceMap;
  private String _initialMaster;

  @BeforeClass
  public void beforeClass() {
    super.beforeClass();
    setup();
    setupIdealState(3, new String[] { _db }, _numPartition, _numReplica,
        IdealState.RebalanceMode.SEMI_AUTO, BuiltInStateModelDefinitions.MasterSlave.name());
    setupStateModel();
    setupInstances(3);
    setupLiveInstances(3);

    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.enableP2PMessage(true);
    setClusterConfig(clusterConfig);

    _dataCache = new ResourceControllerDataProvider(_clusterName);
    _dataCache.setAsyncTasksThreadPool(Executors.newSingleThreadExecutor());

    _dataCache.refresh(manager.getHelixDataAccessor());

    event.addAttribute(AttributeName.ControllerDataProvider.name(), _dataCache);
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    _fullPipeline = new Pipeline("FullPipeline");
    _fullPipeline.addStage(new ReadClusterDataStage());
    _fullPipeline.addStage(new ResourceComputationStage());
    _fullPipeline.addStage(new CurrentStateComputationStage());
    _fullPipeline.addStage(new BestPossibleStateCalcStage());
    _fullPipeline.addStage(new IntermediateStateCalcStage());
    _fullPipeline.addStage(new ResourceMessageGenerationPhase());
    _fullPipeline.addStage(new MessageSelectionStage());
    _fullPipeline.addStage(new MessageThrottleStage());
    _fullPipeline.addStage(new ResourceMessageDispatchStage());

    try {
      _fullPipeline.handle(event);
    } catch (Exception e) {
      e.printStackTrace();
    }

    _instances = _dataCache.getAllInstances();
    _liveInstanceMap = _dataCache.getLiveInstances();

    _initialStateMap = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());
    _initialMaster = getTopStateInstance(_initialStateMap.getInstanceStateMap(_db, _partition),
        MasterSlaveSMD.States.MASTER.name());
    Assert.assertNotNull(_initialMaster);
  }

  @BeforeMethod  // just to overide the per-test setup in base class.
  public void beforeTest(Method testMethod, ITestContext testContext) {
    long startTime = System.currentTimeMillis();
    System.out.println("START " + testMethod.getName() + " at " + new Date(startTime));
    testContext.setAttribute("StartTime", System.currentTimeMillis());
  }

  @Test
  public void testP2PSendAndTimeout() throws Exception {
    reset(_initialStateMap);

    // Disable old master ((initialMaster) instance,
    // Validate: a M->S message should be sent to initialMaster with a P2P message attached for secondMaster.
    admin.enableInstance(_clusterName, _initialMaster, false);
    _dataCache = event.getAttribute(AttributeName.ControllerDataProvider.name());
    _dataCache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);

    _fullPipeline.handle(event);

    ResourcesStateMap bestpossibleState =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    List<Message> messages = getMessages(_initialMaster);

    Assert.assertEquals(messages.size(), 1);
    Message toSlaveMessage = messages.get(0);
    Assert.assertEquals(toSlaveMessage.getTgtName(), _initialMaster);
    Assert.assertEquals(toSlaveMessage.getFromState(), MasterSlaveSMD.States.MASTER.name());
    Assert.assertEquals(toSlaveMessage.getToState(), MasterSlaveSMD.States.SLAVE.name());

    // verify p2p message are attached to the M->S message sent to the old master instance
    Assert.assertEquals(toSlaveMessage.getRelayMessages().entrySet().size(), 1);
    String secondMaster =
        getTopStateInstance(bestpossibleState.getInstanceStateMap(_db, _partition),
            MasterSlaveSMD.States.MASTER.name());

    Message relayMessage = toSlaveMessage.getRelayMessage(secondMaster);
    Assert.assertNotNull(relayMessage);
    Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
    Assert.assertEquals(relayMessage.getTgtName(), secondMaster);
    Assert.assertEquals(relayMessage.getRelaySrcHost(), _initialMaster);
    Assert.assertEquals(relayMessage.getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(relayMessage.getToState(), MasterSlaveSMD.States.MASTER.name());

    // Old master (initialMaster) completed the M->S transition,
    // but has not forward p2p message to new master (secondMaster) yet.
    // Validate: Controller should not send S->M message to new master.
    handleMessage(_initialMaster, _db);
    _fullPipeline.handle(event);
    messages = getMessages(secondMaster);
    Assert.assertEquals(messages.size(), 0);

    // Old master (initialMaster) completed the M->S transition,
    // but has not forward p2p message to new master (secondMaster) yet, but p2p message should already timeout.
    // Validate: Controller should send S->M message to new master.
    Thread.sleep(Message.RELAY_MESSAGE_DEFAULT_EXPIRY);

    _fullPipeline.handle(event);
    messages = getMessages(secondMaster);
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), secondMaster);
    Assert.assertEquals(messages.get(0).getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(messages.get(0).getToState(), MasterSlaveSMD.States.MASTER.name());
  }

  @Test
  public void testP2PWithErrorState() throws Exception {
    reset(_initialStateMap);
    // Disable old master ((initialMaster) instance,
    // Validate: a M->S message should be sent to initialMaster with a P2P message attached for secondMaster.

    // disable existing master instance
    admin.enableInstance(_clusterName, _initialMaster, false);
    _dataCache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);
    _fullPipeline.handle(event);

    ResourcesStateMap bestpossibleState =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    List<Message> messages = getMessages(_initialMaster);

    Assert.assertEquals(messages.size(), 1);
    Message toSlaveMessage = messages.get(0);

    // verify p2p message are attached to the M->S message sent to the old master instance
    Assert.assertEquals(toSlaveMessage.getRelayMessages().entrySet().size(), 1);
    String secondMaster =
        getTopStateInstance(bestpossibleState.getInstanceStateMap(_db, _partition),
            MasterSlaveSMD.States.MASTER.name());
    Message relayMessage = toSlaveMessage.getRelayMessage(secondMaster);
    Assert.assertNotNull(relayMessage);
    Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
    Assert.assertEquals(relayMessage.getTgtName(), secondMaster);

    // Old master (initialMaster) failed the M->S transition,
    // but has not forward p2p message to new master (secondMaster) yet.
    // Validate: Controller should ignore the ERROR partition and send S->M message to new master.
    String session = _dataCache.getLiveInstances().get(_initialMaster).getEphemeralOwner();
    PropertyKey currentStateKey =
        new PropertyKey.Builder(_clusterName).currentState(_initialMaster, session, _db);
    CurrentState currentState = accessor.getProperty(currentStateKey);
    currentState
        .setPreviousState(_partition.getPartitionName(), MasterSlaveSMD.States.MASTER.name());
    currentState.setState(_partition.getPartitionName(), HelixDefinedState.ERROR.name());
    currentState.setEndTime(_partition.getPartitionName(), System.currentTimeMillis());
    accessor.setProperty(currentStateKey, currentState);

    PropertyKey messageKey =
        new PropertyKey.Builder(_clusterName).message(_initialMaster, messages.get(0).getMsgId());
    accessor.removeProperty(messageKey);

    _fullPipeline.handle(event);

    messages = getMessages(secondMaster);

    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), secondMaster);
    Assert.assertEquals(messages.get(0).getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(messages.get(0).getToState(), MasterSlaveSMD.States.MASTER.name());
  }

  @Test
  public void testP2PWithInstanceOffline() throws Exception {
    reset(_initialStateMap);
    // Disable old master ((initialMaster) instance,
    // Validate: a M->S message should be sent to initialMaster with a P2P message attached for secondMaster.

    // disable existing master instance
    admin.enableInstance(_clusterName, _initialMaster, false);
    _dataCache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);
    _fullPipeline.handle(event);

    ResourcesStateMap bestpossibleState =
        event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());

    List<Message> messages = getMessages(_initialMaster);

    Assert.assertEquals(messages.size(), 1);
    Message toSlaveMessage = messages.get(0);
    ;

    // verify p2p message are attached to the M->S message sent to the old master instance
    Assert.assertEquals(toSlaveMessage.getRelayMessages().entrySet().size(), 1);
    String secondMaster =
        getTopStateInstance(bestpossibleState.getInstanceStateMap(_db, _partition),
            MasterSlaveSMD.States.MASTER.name());
    Message relayMessage = toSlaveMessage.getRelayMessage(secondMaster);
    Assert.assertNotNull(relayMessage);
    Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
    Assert.assertEquals(relayMessage.getTgtName(), secondMaster);

    // Old master (initialMaster) completed the M->S transition,
    // but has not forward p2p message to new master (secondMaster) yet.
    // Validate: Controller should not send S->M message to new master.
    handleMessage(_initialMaster, _db);
    _fullPipeline.handle(event);
    messages = getMessages(secondMaster);
    Assert.assertEquals(messages.size(), 0);

    // New master (second master) instance goes offline, controller should send S->M to the third master immediately.
    PropertyKey liveInstanceKey = new PropertyKey.Builder(_clusterName).liveInstance(secondMaster);
    accessor.removeProperty(liveInstanceKey);
    _dataCache.requireFullRefresh();

    _fullPipeline.handle(event);

    bestpossibleState = event.getAttribute(AttributeName.INTERMEDIATE_STATE.name());
    String thirdMaster = getTopStateInstance(bestpossibleState.getInstanceStateMap(_db, _partition),
        MasterSlaveSMD.States.MASTER.name());

    Assert.assertTrue(secondMaster != thirdMaster);
    messages = getMessages(thirdMaster);
    Assert.assertEquals(messages.size(), 1);
    Assert.assertEquals(messages.get(0).getTgtName(), thirdMaster);
    Assert.assertEquals(messages.get(0).getFromState(), MasterSlaveSMD.States.SLAVE.name());
    Assert.assertEquals(messages.get(0).getToState(), MasterSlaveSMD.States.MASTER.name());
  }

  /**
   * This is to simulate the participant (without starting a real participant thread) to handle the pending message.
   * It sets the CurrentState to target State, and remove the pending message from ZK.
   * @param instance
   * @param resource
   */
  private void handleMessage(String instance, String resource) {
    PropertyKey propertyKey = new PropertyKey.Builder(_clusterName).messages(instance);
    List<Message> messages = accessor.getChildValues(propertyKey);
    String session = _dataCache.getLiveInstances().get(instance).getEphemeralOwner();

    for (Message m : messages) {
      if (m.getResourceName().equals(resource)) {
        PropertyKey currentStateKey =
            new PropertyKey.Builder(_clusterName).currentState(instance, session, resource);
        CurrentState currentState = accessor.getProperty(currentStateKey);
        if (currentState == null) {
          currentState = new CurrentState(resource);
          currentState.setSessionId(session);
          currentState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
        }

        String partition = m.getPartitionName();
        String fromState = m.getFromState();
        String toState = m.getToState();
        String partitionState = currentState.getState(partition);

        if ((partitionState == null && fromState.equals(
            BuiltInStateModelDefinitions.MasterSlave.getStateModelDefinition().getInitialState()))
            || (partitionState.equals(fromState))) {
          currentState.setPreviousState(partition, fromState);
          currentState.setState(partition, toState);
          currentState.setStartTime(partition, System.currentTimeMillis());
          try {
            Thread.sleep(50);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          currentState.setEndTime(partition, System.currentTimeMillis());
          accessor.setProperty(currentStateKey, currentState);
          PropertyKey messageKey =
              new PropertyKey.Builder(_clusterName).message(instance, m.getMsgId());
          accessor.removeProperty(messageKey);
        }
      }
    }
  }

  /**
   *  Enable all instances, clean all pending messages, set CurrentState to the BestPossibleState
    */
  private void reset(ResourcesStateMap bestpossibleState) {
    for (String ins : _liveInstanceMap.keySet()) {
      LiveInstance liveInstance = _liveInstanceMap.get(ins);
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      accessor.setProperty(keyBuilder.liveInstance(liveInstance.getId()), liveInstance);
    }
    for (String ins : _instances) {
      admin.enableInstance(_clusterName, _initialMaster, true);
      cleanMessages(ins);
    }
    for (String resource : bestpossibleState.resourceSet()) {
      setCurrentState(resource, bestpossibleState.getPartitionStateMap(resource).getStateMap());
    }
    for (String ins : _instances) {
      cleanMessages(ins);
    }
    _dataCache.requireFullRefresh();
  }

  private void setCurrentState(String resource,
      Map<Partition, Map<String, String>> partitionStateMap) {
    for (Partition p : partitionStateMap.keySet()) {
      Map<String, String> partitionState = partitionStateMap.get(p);
      for (String instance : partitionState.keySet()) {
        String state = partitionState.get(instance);
        String session = _liveInstanceMap.get(instance).getEphemeralOwner();
        PropertyKey currentStateKey =
            new PropertyKey.Builder(_clusterName).currentState(instance, session, resource);
        CurrentState currentState = accessor.getProperty(currentStateKey);
        if (currentState == null) {
          currentState = new CurrentState(resource);
          currentState.setSessionId(session);
          currentState.setStateModelDefRef(BuiltInStateModelDefinitions.MasterSlave.name());
        }
        currentState.setState(p.getPartitionName(), state);
        accessor.setProperty(currentStateKey, currentState);
      }
    }
  }

  private void cleanMessages(String instance) {
    PropertyKey propertyKey = new PropertyKey.Builder(_clusterName).messages(instance);
    List<Message> messages = accessor.getChildValues(propertyKey);
    for (Message m : messages) {
      accessor
          .removeProperty(new PropertyKey.Builder(_clusterName).message(instance, m.getMsgId()));
    }
  }

  List<Message> getMessages(String instance) {
    return accessor.getChildValues(new PropertyKey.Builder(_clusterName).messages(instance));
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
}
