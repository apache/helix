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
import org.apache.helix.HelixConstants;
import org.apache.helix.controller.common.PartitionStateMap;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.BestPossibleStateOutput;
import org.apache.helix.controller.stages.ClusterDataCache;
import org.apache.helix.controller.stages.CurrentStateOutput;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.MessageSelectionStageOutput;
import org.apache.helix.controller.stages.MessageThrottleStage;
import org.apache.helix.controller.stages.ReadClusterDataStage;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestP2PStateTransitionMessages extends BaseStageTest {
  String db = "testDB";
  int numPartition = 1;
  int numReplica = 3;


  private void preSetup() {
    setupIdealState(3, new String[]{db}, numPartition, numReplica, IdealState.RebalanceMode.SEMI_AUTO,
        BuiltInStateModelDefinitions.MasterSlave.name());
    setupStateModel();
    setupInstances(3);
    setupLiveInstances(3);
  }

  @Test
  public void testP2PMessageEnabled() throws Exception {
    preSetup();
    ClusterConfig clusterConfig = new ClusterConfig(_clusterName);
    clusterConfig.enableP2PMessage(true);
    setClusterConfig(clusterConfig);

    testP2PMessage(clusterConfig, true);
  }

  @Test
  public void testP2PMessageDisabled() throws Exception {
    preSetup();
    testP2PMessage(null, false);
  }

  private void testP2PMessage(ClusterConfig clusterConfig, Boolean p2pMessageEnabled) throws Exception {
    Map<String, Resource> resourceMap =
        getResourceMap(new String[]{db}, numPartition, BuiltInStateModelDefinitions.MasterSlave.name(), clusterConfig,
            null);

    event.addAttribute(AttributeName.RESOURCES.name(), resourceMap);
    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), new CurrentStateOutput());
    event.addAttribute(AttributeName.helixmanager.name(), manager);

    Pipeline pipeline = createPipeline();
    pipeline.handle(event);

    BestPossibleStateOutput bestPossibleStateOutput = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    CurrentStateOutput currentStateOutput = populateCurrentStateFromBestPossible(bestPossibleStateOutput);
    event.addAttribute(AttributeName.CURRENT_STATE.name(), currentStateOutput);

    Partition p = new Partition(db + "_0");

    String masterInstance =
        getTopStateInstance(bestPossibleStateOutput.getInstanceStateMap(db, p), MasterSlaveSMD.States.MASTER.name());
    Assert.assertNotNull(masterInstance);

    admin.enableInstance(_clusterName, masterInstance, false);
    ClusterDataCache cache = event.getAttribute(AttributeName.ClusterDataCache.name());
    cache.notifyDataChange(HelixConstants.ChangeType.INSTANCE_CONFIG);

    pipeline.handle(event);

    bestPossibleStateOutput = event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.name());

    MessageSelectionStageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    List<Message> messages = messageOutput.getMessages(db, p);

    Assert.assertEquals(messages.size(), 1);
    Message message = messages.get(0);
    Assert.assertEquals(message.getTgtName(), masterInstance);
    Assert.assertEquals(message.getFromState(), MasterSlaveSMD.States.MASTER.name());
    Assert.assertEquals(message.getToState(), MasterSlaveSMD.States.SLAVE.name());

    if (p2pMessageEnabled) {
      Assert.assertEquals(message.getRelayMessages().entrySet().size(), 1);
      String newMasterInstance =
          getTopStateInstance(bestPossibleStateOutput.getInstanceStateMap(db, p), MasterSlaveSMD.States.MASTER.name());

      Message relayMessage = message.getRelayMessage(newMasterInstance);
      Assert.assertNotNull(relayMessage);
      Assert.assertEquals(relayMessage.getMsgSubType(), Message.MessageType.RELAYED_MESSAGE.name());
      Assert.assertEquals(relayMessage.getTgtName(), newMasterInstance);
      Assert.assertEquals(relayMessage.getRelaySrcHost(), masterInstance);
      Assert.assertEquals(relayMessage.getFromState(), MasterSlaveSMD.States.SLAVE.name());
      Assert.assertEquals(relayMessage.getToState(), MasterSlaveSMD.States.MASTER.name());
    } else {
      Assert.assertTrue(message.getRelayMessages().entrySet().isEmpty());
    }
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

  private CurrentStateOutput populateCurrentStateFromBestPossible(BestPossibleStateOutput bestPossibleStateOutput) {
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

  private Pipeline createPipeline() {
    Pipeline pipeline = new Pipeline("test");
    pipeline.addStage(new ReadClusterDataStage());
    pipeline.addStage(new BestPossibleStateCalcStage());
    pipeline.addStage(new IntermediateStateCalcStage());
    pipeline.addStage(new MessageGenerationPhase());
    pipeline.addStage(new MessageSelectionStage());
    pipeline.addStage(new MessageThrottleStage());

    return pipeline;
  }
}
