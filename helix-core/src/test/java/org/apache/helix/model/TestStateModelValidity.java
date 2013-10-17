package org.apache.helix.model;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.HelixDefinedState;
import org.apache.helix.ZNRecord;
import org.apache.helix.model.StateModelDefinition.StateModelDefinitionProperty;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class TestStateModelValidity {
  /**
   * Ensure that state models that we know to be good pass validation
   */
  @Test
  public void testValidModels() {
    StateModelDefinition masterSlave =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForMasterSlave());
    Assert.assertTrue(masterSlave.isValid());

    StateModelDefinition leaderStandby =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby());
    Assert.assertTrue(leaderStandby.isValid());

    StateModelDefinition onlineOffline =
        new StateModelDefinition(StateModelConfigGenerator.generateConfigForOnlineOffline());
    Assert.assertTrue(onlineOffline.isValid());
  }

  /**
   * Ensure that Helix responds negatively if DROPPED is not specified
   */
  @Test
  public void testNoDroppedState() {
    StateModelDefinition stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addTransition("OFFLINE", "SLAVE")
            .addTransition("SLAVE", "MASTER").addTransition("MASTER", "SLAVE")
            .addTransition("SLAVE", "OFFLINE").build();
    Assert.assertFalse(stateModel.isValid());
  }

  /**
   * Ensure that Helix can catch when a state doesn't have a path to DROPPED
   */
  @Test
  public void testNoPathToDropped() {
    StateModelDefinition stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addState("DROPPED")
            .addTransition("OFFLINE", "SLAVE").addTransition("SLAVE", "MASTER")
            .addTransition("SLAVE", "OFFLINE").addTransition("OFFLINE", "DROPPED").build();
    Assert.assertFalse(stateModel.isValid());

    // now see that adding MASTER-DROPPED fixes the problem
    stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addState("DROPPED")
            .addTransition("OFFLINE", "SLAVE").addTransition("SLAVE", "MASTER")
            .addTransition("SLAVE", "OFFLINE").addTransition("OFFLINE", "DROPPED")
            .addTransition("MASTER", "DROPPED").build();
    Assert.assertTrue(stateModel.isValid());
  }

  /**
   * The initial state should be added as a state, otherwise validation check should fail
   */
  @Test
  public void testInitialStateIsNotState() {
    StateModelDefinition stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("MASTER")
            .addState("SLAVE").addState("DROPPED").addTransition("OFFLINE", "SLAVE")
            .addTransition("SLAVE", "MASTER").addTransition("SLAVE", "OFFLINE")
            .addTransition("OFFLINE", "DROPPED").addTransition("MASTER", "SLAVE").build();
    Assert.assertFalse(stateModel.isValid());
  }

  /**
   * There should be an initial state, otherwise instantiation should fail
   */
  @Test
  public void testNoInitialState() {
    try {
      new StateModelDefinition.Builder("stateModel").addState("OFFLINE").addState("MASTER")
          .addState("SLAVE").addState("DROPPED").addTransition("OFFLINE", "SLAVE")
          .addTransition("SLAVE", "MASTER").addTransition("SLAVE", "OFFLINE")
          .addTransition("OFFLINE", "DROPPED").addTransition("MASTER", "SLAVE").build();
      Assert.fail("StateModelDefinition creation should fail if no initial state");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * SRC and DEST in a transition SRC-TEST must be valid states
   */
  @Test
  public void testTransitionsWithInvalidStates() {
    // invalid to state
    StateModelDefinition stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addState("DROPPED")
            .addTransition("OFFLINE", "SLAVE").addTransition("SLAVE", "MASTER")
            .addTransition("SLAVE", "OFFLINE").addTransition("OFFLINE", "DROPPED")
            .addTransition("MASTER", "SLAVE").addTransition("OFFLINE", "INVALID").build();
    Assert.assertFalse(stateModel.isValid());

    // invalid from state
    stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addState("DROPPED")
            .addTransition("OFFLINE", "SLAVE").addTransition("SLAVE", "MASTER")
            .addTransition("SLAVE", "OFFLINE").addTransition("OFFLINE", "DROPPED")
            .addTransition("MASTER", "SLAVE").addTransition("INVALID", "MASTER").build();
    Assert.assertFalse(stateModel.isValid());
  }

  /**
   * The initial state should be able to reach all states, should fail validation otherwise
   */
  @Test
  public void testUnreachableState() {
    StateModelDefinition stateModel =
        new StateModelDefinition.Builder("stateModel").initialState("OFFLINE").addState("OFFLINE")
            .addState("MASTER").addState("SLAVE").addState("DROPPED")
            .addTransition("OFFLINE", "SLAVE").addTransition("SLAVE", "OFFLINE")
            .addTransition("OFFLINE", "DROPPED").addTransition("MASTER", "SLAVE")
            .addTransition("MASTER", "DROPPED").build();
    Assert.assertFalse(stateModel.isValid());
  }

  /**
   * The validator should fail on any detected infinite loops
   */
  @Test
  public void testLoopInStateModel() {
    // create an infinite loop ONE --> TWO --> ONE
    ZNRecord record = new ZNRecord("MasterSlave");
    record.setSimpleField(StateModelDefinitionProperty.INITIAL_STATE.toString(), "OFFLINE");
    List<String> statePriorityList =
        Lists.newArrayList("ONE", "TWO", "THREE", "OFFLINE", "DROPPED", "ERROR");
    record.setListField(StateModelDefinitionProperty.STATE_PRIORITY_LIST.toString(),
        statePriorityList);
    for (String state : statePriorityList) {
      String key = state + ".meta";
      Map<String, String> metadata = new HashMap<String, String>();
      metadata.put("count", "-1");
      record.setMapField(key, metadata);
    }
    for (String state : statePriorityList) {
      String key = state + ".next";
      if (state.equals("ONE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("THREE", "TWO");
        metadata.put("TWO", "TWO");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("TWO")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("THREE", "ONE");
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("THREE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        metadata.put("DROPPED", "OFFLINE");
        record.setMapField(key, metadata);
      } else if (state.equals("OFFLINE")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("ONE", "ONE");
        metadata.put("TWO", "TWO");
        metadata.put("THREE", "THREE");
        metadata.put("DROPPED", "DROPPED");
        record.setMapField(key, metadata);
      } else if (state.equals("ERROR")) {
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("OFFLINE", "OFFLINE");
        record.setMapField(key, metadata);
      }
    }
    List<String> stateTransitionPriorityList = new ArrayList<String>();
    record.setListField(StateModelDefinitionProperty.STATE_TRANSITION_PRIORITYLIST.toString(),
        stateTransitionPriorityList);

    StateModelDefinition stateModel = new StateModelDefinition(record);
    Assert.assertFalse(stateModel.isValid());
  }

  /**
   * This is the example used on the website, so this must work
   */
  @Test
  public void testBasic() {
    StateModelDefinition stateModel = new StateModelDefinition.Builder("MasterSlave")
    // OFFLINE is the state that the system starts in (initial state is REQUIRED)
        .initialState("OFFLINE")

        // Lowest number here indicates highest priority, no value indicates lowest priority
        .addState("MASTER", 1).addState("SLAVE", 2).addState("OFFLINE")

        // Note the special inclusion of the DROPPED state (REQUIRED)
        .addState(HelixDefinedState.DROPPED.toString())

        // No more than one master allowed
        .upperBound("MASTER", 1)

        // R indicates an upper bound of number of replicas for each partition
        .dynamicUpperBound("SLAVE", "R")

        // Add some high-priority transitions
        .addTransition("SLAVE", "MASTER", 1).addTransition("OFFLINE", "SLAVE", 2)

        // Using the same priority value indicates that these transitions can fire in any order
        .addTransition("MASTER", "SLAVE", 3).addTransition("SLAVE", "OFFLINE", 3)

        // Not specifying a value defaults to lowest priority
        // Notice the inclusion of the OFFLINE to DROPPED transition
        // Since every state has a path to OFFLINE, they each now have a path to DROPPED (REQUIRED)
        .addTransition("OFFLINE", HelixDefinedState.DROPPED.toString())

        // Create the StateModelDefinition instance
        .build();

    Assert.assertTrue(stateModel.isValid());
  }
}
