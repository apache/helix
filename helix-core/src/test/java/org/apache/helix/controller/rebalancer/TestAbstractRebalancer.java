package org.apache.helix.controller.rebalancer;

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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.util.TestInputLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


public class TestAbstractRebalancer {

  @Test(dataProvider = "TestComputeBestPossibleStateInput")
  public void testComputeBestPossibleState(String comment, String stateModelName, List<String> liveInstances,
      List<String> preferenceList, Map<String, String> currentStateMap, List<String> disabledInstancesForPartition,
      Map<String, String> expectedBestPossibleMap) {
    System.out.println("Test case comment: " + comment);
    AutoRebalancer rebalancer = new AutoRebalancer();
    Map<String, String> bestPossibleMap = rebalancer
        .computeBestPossibleStateForPartition(new HashSet<String>(liveInstances),
            BuiltInStateModelDefinitions.valueOf(stateModelName).getStateModelDefinition(),
            preferenceList, currentStateMap, new HashSet<String>(disabledInstancesForPartition), true);

    Assert.assertTrue(bestPossibleMap.equals(expectedBestPossibleMap));
  }

  @DataProvider(name = "TestComputeBestPossibleStateInput")
  public Object[][] loadTestComputeBestPossibleStateInput() {
    final String[] params = {"comment", "stateModel", "liveInstances", "preferenceList", "currentStateMap",
        "disabledInstancesForPartition", "expectedBestPossibleStateMap"};
    return TestInputLoader.loadTestInputs("TestAbstractRebalancer.ComputeBestPossibleState.json", params);
  }
}
