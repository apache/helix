package org.apache.helix.integration.controller;

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
import java.util.Map;

import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.stages.AttributeName;
import org.apache.helix.controller.stages.BestPossibleStateCalcStage;
import org.apache.helix.controller.stages.ClusterEvent;
import org.apache.helix.controller.stages.ClusterEventType;
import org.apache.helix.controller.stages.CurrentStateComputationStage;
import org.apache.helix.controller.stages.IntermediateStateCalcStage;
import org.apache.helix.controller.stages.MessageGenerationPhase;
import org.apache.helix.controller.stages.MessageOutput;
import org.apache.helix.controller.stages.MessageSelectionStage;
import org.apache.helix.controller.stages.ResourceComputationStage;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Partition;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRedundantDroppedMessage extends TaskSynchronizedTestBase {
  @BeforeClass
  public void beforeClass() throws Exception {
    _numNodes = 2;
    _numReplicas = 1;
    _numDbs = 1;
    _numPartitions = 1;
    super.beforeClass();
  }

  @Test
  public void testNoRedundantDropMessage() throws Exception {
    String resourceName = "TEST_RESOURCE";
    _gSetupTool.getClusterManagementTool().addResource(CLUSTER_NAME, resourceName, 1, "MasterSlave",
        IdealState.RebalanceMode.CUSTOMIZED.name());
    String partitionName = "P_0";
    ClusterEvent event = new ClusterEvent(CLUSTER_NAME, ClusterEventType.Unknown, "ID");
    ResourceControllerDataProvider cache = new ResourceControllerDataProvider(CLUSTER_NAME);
    cache.refresh(_manager.getHelixDataAccessor());
    IdealState idealState = cache.getIdealState(resourceName);
    idealState.setReplicas("2");
    Map<String, String> stateMap = new HashMap<>();
    stateMap.put(_participants[0].getInstanceName(), "SLAVE");
    stateMap.put(_participants[1].getInstanceName(), "DROPPED");
    idealState.setInstanceStateMap(partitionName, stateMap);

    cache.setIdealStates(Arrays.asList(idealState));
    cache.setCachedIdealMapping(idealState.getResourceName(), idealState.getRecord());

    event.addAttribute(AttributeName.ControllerDataProvider.name(), cache);
    event.addAttribute(AttributeName.helixmanager.name(), _manager);

    runStage(event, new ResourceComputationStage());
    runStage(event, new CurrentStateComputationStage());
    runStage(event, new BestPossibleStateCalcStage());
    Assert.assertEquals(cache.getCachedIdealMapping().size(), 1);
    runStage(event, new MessageGenerationPhase());
    runStage(event, new MessageSelectionStage());
    runStage(event, new IntermediateStateCalcStage());


    MessageOutput messageOutput = event.getAttribute(AttributeName.MESSAGES_SELECTED.name());
    Assert
        .assertEquals(messageOutput.getMessages(resourceName, new Partition(partitionName)).size(),
            1);
    Assert.assertEquals(cache.getCachedIdealMapping().size(), 0);
  }



}
