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
import java.util.List;
import java.util.Map;

import org.apache.helix.ZNRecord;
import org.apache.helix.PropertyKey.Builder;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.IdealState.IdealStateModeProperty;
import org.apache.helix.model.Partition;
import org.apache.helix.model.Resource;
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
    System.out.println("START TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));

    String[] resources = new String[] {
      "testResourceName"
    };
    setupIdealStateDeprecated(5, resources, 10, 1, IdealStateModeProperty.AUTO);
    setupLiveInstances(5);
    setupStateModel();

    Map<String, Resource> resourceMap = getResourceMap();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++) {
      Partition resource = new Partition("testResourceName_" + p);
      AssertJUnit.assertEquals("MASTER", output.getInstanceStateMap("testResourceName", resource)
          .get("localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testCustomModeCompatibility() {
    System.out.println("START TestBestPossibleStateCalcStage at "
        + new Date(System.currentTimeMillis()));

    String[] resources = new String[] {
      "testResourceName"
    };
    setupIdealStateDeprecated(5, resources, 10, 1, IdealStateModeProperty.CUSTOMIZED);
    setupLiveInstances(5);
    setupStateModel();

    Map<String, Resource> resourceMap = getResourceMap();
    CurrentStateOutput currentStateOutput = new CurrentStateOutput();
    event.addAttribute(AttributeName.RESOURCES.toString(), resourceMap);
    event.addAttribute(AttributeName.CURRENT_STATE.toString(), currentStateOutput);

    ReadClusterDataStage stage1 = new ReadClusterDataStage();
    runStage(event, stage1);
    BestPossibleStateCalcStage stage2 = new BestPossibleStateCalcStage();
    runStage(event, stage2);

    BestPossibleStateOutput output =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (int p = 0; p < 5; p++) {
      Partition resource = new Partition("testResourceName_" + p);
      AssertJUnit.assertNull(output.getInstanceStateMap("testResourceName", resource).get(
          "localhost_" + (p + 1) % 5));
    }
    System.out.println("END TestBestPossibleStateCalcStage at "
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
      ZNRecord record = new ZNRecord(resourceName);
      for (int p = 0; p < partitions; p++) {
        List<String> value = new ArrayList<String>();
        for (int r = 0; r < replicas; r++) {
          value.add("localhost_" + (p + r + 1) % nodes);
        }
        record.setListField(resourceName + "_" + p, value);
      }
      IdealState idealState = new IdealState(record);
      idealState.setStateModelDefRef("MasterSlave");
      idealState.setIdealStateMode(mode.toString());
      idealState.setNumPartitions(partitions);
      idealStates.add(idealState);

      // System.out.println(idealState);

      Builder keyBuilder = accessor.keyBuilder();

      accessor.setProperty(keyBuilder.idealStates(resourceName), idealState);
    }
    return idealStates;
  }
}
