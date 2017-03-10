package org.apache.helix.controller.rebalancer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.controller.stages.BaseStageTest;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.StateModelDefinition;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

public class TestZeroReplicaAvoidance extends BaseStageTest {

  @Test(dataProvider = "zeroReplicaInput")
  public void testZeroReplicaAvoidanceDuringRebalance(StateModelDefinition stateModelDef,
      List<String> instancePreferenceList, Map<String, String> currentStateMap,
      Map<String, String> expectedBestPossibleMap) {
    System.out.println("START TestDelayedAutoRebalancer at " + new Date(System.currentTimeMillis()));

    System.err.println("Test input: " + instancePreferenceList + ":" + currentStateMap + ":");

    int numNode = 6;
    Set<String> liveInstances = new HashSet<String>();
    for (int i = 0; i < numNode; i++) {
      liveInstances.add("localhost_" + i);
    }

    DelayedAutoRebalancer rebalancer = new DelayedAutoRebalancer();
    Map<String, String> bestPossibleMap = rebalancer
        .computeBestPossibleStateForPartition(liveInstances, stateModelDef, instancePreferenceList, currentStateMap,
            Collections.<String>emptySet(), true);
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap,
        "Differs, get " + bestPossibleMap + ": expected: " + expectedBestPossibleMap);

    System.out.println("END TestBestPossibleStateCalcStage at " + new Date(System.currentTimeMillis()));
  }

  @DataProvider(name = "zeroReplicaInput")
  public Object[][] rebalanceStrategies() {
    List<Object[]> data = new ArrayList<Object[]>();
    data.addAll(loadTestInputs("TestDelayedAutoRebalancer.MasterSlave.json"));
    data.addAll(loadTestInputs("TestDelayedAutoRebalancer.OnlineOffline.json"));

    Object[][] ret = new Object[data.size()][];
    for(int i = 0; i < data.size(); i++) {
      ret[i] = data.get(i);
    }
    return ret;
  }

  private final String INPUT = "inputs";
  private final String CURRENT_STATE = "currentStates";
  private final String BEST_POSSIBLE_STATE = "bestPossibleStates";
  private final String PREFERENCE_LIST = "preferenceList";
  private final String STATE_MODEL = "statemodel";

  public List<Object[]> loadTestInputs(String fileName) {
    List<Object[]> ret = null;
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName);
    try {
      ObjectReader mapReader = new ObjectMapper().reader(Map.class);
      Map<String, Object> inputMaps = mapReader.readValue(inputStream);
      String stateModelName = (String) inputMaps.get(STATE_MODEL);

      StateModelDefinition stateModelDef =
          BuiltInStateModelDefinitions.valueOf(stateModelName).getStateModelDefinition();

      List<Map<String, Object>> inputs = (List<Map<String, Object>>) inputMaps.get(INPUT);
      ret = new ArrayList<Object[]>();
      for (Map<String, Object> inMap : inputs) {
        Map<String, String> currentStates = (Map<String, String>) inMap.get(CURRENT_STATE);
        Map<String, String> bestPossibleStates =
            (Map<String, String>) inMap.get(BEST_POSSIBLE_STATE);
        List<String> preferenceList = (List<String>) inMap.get(PREFERENCE_LIST);

        ret.add(new Object[] { stateModelDef, preferenceList, currentStates, bestPossibleStates });
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return ret;
  }
}
