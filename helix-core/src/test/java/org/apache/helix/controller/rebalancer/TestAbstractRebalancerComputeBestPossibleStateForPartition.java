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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.StateModelDefinition;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


// For recovery from <Offline,Slave> to <Master,Slave>,
// but we first transit to <Slave,Master>, then <Master,Slave>,
// in order to increase availability
public class TestAbstractRebalancerComputeBestPossibleStateForPartition {

  private StateModelDefinition stateModelDefinition = MasterSlaveSMD.build();
  private final String NODE_0 = "NODE_0", NODE_1 = "NODE_1";
  private final String MASTER = "MASTER", SLAVE = "SLAVE", OFFLINE = "OFFLINE";
  private List<String> preferenceList = Lists.newArrayList(NODE_0, NODE_1);
  private Set<String> disabledInstances = new HashSet<String>();
  private Map<String, String> currentStateMap = new HashMap<String, String>();
  private Map<String, String> expectedBestPossibleMap = new HashMap<String, String>();
  private Set<String> liveInstance = Sets.newHashSet(NODE_0, NODE_1);
  private AbstractRebalancer rebalancer = new AutoRebalancer();

  @BeforeMethod
  public void clearMaps() {
    currentStateMap.clear();
    expectedBestPossibleMap.clear();
  }

  @Test
  public void case1() {
    // Promote to Slave, at the same time promote another Slave to Master
    currentStateMap.put(NODE_0, OFFLINE);
    currentStateMap.put(NODE_1, SLAVE);
    expectedBestPossibleMap.put(NODE_0, SLAVE);
    expectedBestPossibleMap.put(NODE_1, MASTER);

    Map<String, String> bestPossibleMap =
        rebalancer.computeBestPossibleStateForPartition(stateModelDefinition, preferenceList, currentStateMap,
            liveInstance, disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap);
  }

  @Test
  public void case2() {
    // Be able to switch Master and Slave
    currentStateMap.put(NODE_0, SLAVE);
    currentStateMap.put(NODE_1, MASTER);
    expectedBestPossibleMap.put(NODE_0, MASTER);
    expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        rebalancer.computeBestPossibleStateForPartition(stateModelDefinition, preferenceList, currentStateMap,
            liveInstance, disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap);
  }

  @Test
  public void case3() {
    // Stay the same
    currentStateMap.put(NODE_0, MASTER);
    currentStateMap.put(NODE_1, SLAVE);
    expectedBestPossibleMap.put(NODE_0, MASTER);
    expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        rebalancer.computeBestPossibleStateForPartition(stateModelDefinition, preferenceList, currentStateMap,
            liveInstance, disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap);
  }

  @Test
  public void case4() {
    // Promote normally
    currentStateMap.put(NODE_0, OFFLINE);
    currentStateMap.put(NODE_1, OFFLINE);
    expectedBestPossibleMap.put(NODE_0, MASTER);
    expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        rebalancer.computeBestPossibleStateForPartition(stateModelDefinition, preferenceList, currentStateMap,
            liveInstance, disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, expectedBestPossibleMap);
  }
}
