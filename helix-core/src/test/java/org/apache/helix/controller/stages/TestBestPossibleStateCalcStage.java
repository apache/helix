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
import java.util.Map;

import org.apache.helix.api.State;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.RebalanceMode;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestBestPossibleStateCalcStage extends BaseStageTest {
  @Test
  public void testSimple() {
    System.out.println("START TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
    // List<IdealState> idealStates = new ArrayList<IdealState>();

    String[] resources = new String[] {
      "testResourceName"
    };
    List<IdealState> idealStates = setupIdealState(5, resources, 10, 1, RebalanceMode.SEMI_AUTO);
    setupLiveInstances(5);
    setupStateModel();

    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap(idealStates);
    ResourceCurrentState currentStateOutput = new ResourceCurrentState();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++) {
      Map<ParticipantId, State> replicaMap =
          output.getResourceAssignment(ResourceId.from("testResourceName")).getReplicaMap(
              PartitionId.from("testResourceName_" + p));
      AssertJUnit.assertEquals(State.from("MASTER"),
          replicaMap.get(ParticipantId.from("localhost_" + (p + 1) % 5)));
    }
    System.out.println("END TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
  }
}
