package org.apache.helix.util;

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
import java.util.List;
import java.util.Map;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class TestIdealStateAssignment {
  private static final String fileNamePrefix = "TestIdealStateAssignment";

  @Test(dataProvider = "IdealStateInput")
  public void testIdealStateAssignment(String clusterName,
      List<String> instances, List<String> partitions, String numReplicas, String stateModeDef,
      String strategyName, Map<String, Map<String, String>> expectedMapping)
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    ClusterConfig clusterConfig = new ClusterConfig(clusterName);
    List<InstanceConfig> instanceConfigs = new ArrayList<>();
    for (String instance : instances) {
      instanceConfigs.add(new InstanceConfig(instance));
    }

    IdealState idealState = new IdealState("TestResource");
    idealState.setStateModelDefRef(stateModeDef);
    idealState.setNumPartitions(partitions.size());
    idealState.setReplicas(numReplicas);

    Map<String, Map<String, String>> idealStateMapping = HelixUtil
        .getIdealAssignmentForFullAuto(clusterConfig, instanceConfigs, instances, idealState,
            partitions, strategyName);
    Assert.assertEquals(idealStateMapping, expectedMapping);
  }

  @DataProvider(name = "IdealStateInput")
  public Object[][] getIdealStateInput() {
    final String[] inputs =
        { "ClusterName", "Instances", "Partitions", "NumReplica", "StateModelDef", "StrategyName",
            "ExpectedMapping"
        };
    return TestInputLoader.loadTestInputs(fileNamePrefix + ".NoIdealState.json", inputs);
  }
}
