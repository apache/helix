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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.api.Id;
import org.apache.helix.api.ParticipantId;
import org.apache.helix.api.ResourceConfig;
import org.apache.helix.api.ResourceId;
import org.apache.helix.api.State;
import org.apache.helix.api.StateModelDefId;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.StateModelDefinition;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@SuppressWarnings("deprecation")
/**
 * These tests ensure that BestPossibleStateCalcStage correctly recognizes the mode and follows
 * appropriate code paths, even though the old method of setting rebalance mode is used.
 */
public class TestBestPossibleCalcStageCompatibility extends BaseStageTest {
  @Test
  public void testSemiAutoModeCompatibility() {
    System.out
        .println("START TestBestPossibleStateCalcStageCompatibility_testSemiAutoModeCompatibility at "
            + new Date(System.currentTimeMillis()));

    String[] resources = new String[] {
      "testResourceName"
    };
    setupIdealStateDeprecated(5, resources, 10, 1, IdealStateModeProperty.AUTO);
    setupLiveInstances(5);
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = setupStateModel();

    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap();
    NewCurrentStateOutput currentStateOutput = new NewCurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
    event.addAttribute(AttributeName.STATE_MODEL_DEFINITIONS.toString(), stateModelDefs);

    NewReadClusterDataStage stage1 = new NewReadClusterDataStage();
    runStage(event, stage1);
    NewBestPossibleStateCalcStage stage2 = new NewBestPossibleStateCalcStage();
    runStage(event, stage2);

    NewBestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++) {
      Map<ParticipantId, State> replicaMap =
          output.getResourceAssignment(Id.resource("testResourceName")).getReplicaMap(
              Id.partition("testResourceName_" + p));
      AssertJUnit.assertEquals(State.from("MASTER"),
          replicaMap.get(Id.participant("localhost_" + (p + 1) % 5)));
    }
    System.out
        .println("END TestBestPossibleStateCalcStageCompatibility_testSemiAutoModeCompatibility at "
            + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCustomModeCompatibility() {
    System.out
        .println("START TestBestPossibleStateCalcStageCompatibility_testCustomModeCompatibility at "
            + new Date(System.currentTimeMillis()));

    String[] resources = new String[] {
      "testResourceName"
    };
    setupIdealStateDeprecated(5, resources, 10, 1, IdealStateModeProperty.CUSTOMIZED);
    setupLiveInstances(5);
    Map<StateModelDefId, StateModelDefinition> stateModelDefs = setupStateModel();

    Map<ResourceId, ResourceConfig> resourceMap = getResourceMap();
    NewCurrentStateOutput currentStateOutput = new NewCurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);
    event.addAttribute(AttributeName.STATE_MODEL_DEFINITIONS.toString(), stateModelDefs);

    NewReadClusterDataStage stage1 = new NewReadClusterDataStage();
    runStage(event, stage1);
    NewBestPossibleStateCalcStage stage2 = new NewBestPossibleStateCalcStage();
    runStage(event, stage2);

    NewBestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++) {
      Map<ParticipantId, State> replicaMap =
          output.getResourceAssignment(Id.resource("testResourceName")).getReplicaMap(
              Id.partition("testResourceName_" + p));
      AssertJUnit.assertEquals(State.from("MASTER"),
          replicaMap.get(Id.participant("localhost_" + (p + 1) % 5)));
    }
    System.out
        .println("END TestBestPossibleStateCalcStageCompatibility_testCustomModeCompatibility at "
            + new Date(System.currentTimeMillis()));
  }

  protected List<IdealState> setupIdealStateDeprecated(int nodes, String[] resources,
      int partitions, int replicas, IdealStateModeProperty mode) {
    List<IdealState> idealStates = new ArrayList<IdealState>();
    List<String> instances = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      instances.add("localhost_" + i);
    }

    for (int i = 0; i < resources.length; i++) {
      String resourceName = resources[i];
      IdealState idealState = new IdealState(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++) {
          value.add("localhost_" + (p + r + 1) % nodes);
        }
        idealState.setPreferenceList(resourceName + "_" + p, value);
        Map<ParticipantId, State> preferenceMap = new HashMap<ParticipantId, State>();
        preferenceMap.put(Id.participant("localhost_" + (p + 1) % 5), State.from("MASTER"));
        idealState.setParticipantStateMap(
            Id.partition(Id.resource(resourceName), Integer.toString(p)), preferenceMap);
      }
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setIdealStateMode(mode.toString());
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);

      Builder keyBuilder = accessor.keyBuilder();

      accessor.setProperty(keyBuilder.idealState(resourceName), idealState);
    }
    return idealStates;
  }
}
