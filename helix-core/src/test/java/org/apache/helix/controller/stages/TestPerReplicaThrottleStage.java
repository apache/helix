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
import org.apache.helix.HelixException;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.pipeline.Stage;
import org.apache.helix.controller.pipeline.StageContext;
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

  // null case, make sure the messages would pass without any throttle
  @Test
  public void testRecoverySlave() {
    String resourcePrefix = "resource";
    int nResource = 1;
    int nPartition = 1;
    int nReplica = 3;
    String[] resources = new String[nResource];
    for (int i = 0; i < nResource; i++) {
      resources[i] = resourcePrefix + "-" + i;
    }

    preSetup(resources, nPartition, nReplica);

    event.addAttribute(AttributeName.RESOURCES_TO_REBALANCE.name(),
        getResourceMap(resources, nPartition, "MasterSlave"));

    // setup current state; setup message output; setup best possible
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    MessageOutput messageOutput = new MessageOutput();
    BestPossibleStateOutput bestPossibleStateOutput = new BestPossibleStateOutput();
    for (String resource : resources) {
      for (int p = 0; p < nPartition; p++) {
        Partition partition = new Partition(resource + "_" + p);
        currentStateOutput.setCurrentState(resource, partition, HOSTNAME_PREFIX + 0, "OFFLINE");
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
    runStage(event, new PerReplicaThrottleStage());

    MessageOutput output = event.getAttribute(AttributeName.PER_REPLICA_OUTPUT_MESSAGES.name());
    Partition partition = new Partition(resources[0] + "_0");
    List<Message> msgs = output.getMessages(resources[0], partition);
    Assert.assertTrue(msgs.size() == 1);
    Message msg = msgs.get(0);
    Assert.assertTrue(msg.getId().equals("001"));
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

