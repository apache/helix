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

  private static final String NODE_0 = "NODE_0";
  private static final String NODE_1 = "NODE_1";
  private static final String MASTER = MasterSlaveSMD.States.MASTER.toString();
  private static final String SLAVE = MasterSlaveSMD.States.SLAVE.toString();
  private static final String OFFLINE = MasterSlaveSMD.States.OFFLINE.toString();

  private StateModelDefinition _stateModelDefinition = MasterSlaveSMD.build();
  private List<String> _preferenceList = Lists.newArrayList(NODE_0, NODE_1);
  private Set<String> _disabledInstances = new HashSet<String>();
  private Map<String, String> _currentStateMap = new HashMap<String, String>();
  private Map<String, String> _expectedBestPossibleMap = new HashMap<String, String>();
  private Set<String> _liveInstance = Sets.newHashSet(NODE_0, NODE_1);
  private AbstractRebalancer _rebalancer = new AutoRebalancer();

  @BeforeMethod
  public void clearMaps() {
    _currentStateMap.clear();
    _expectedBestPossibleMap.clear();
  }

  @Test
  public void case1() {
    // Promote to Slave, at the same time promote another Slave to Master
    _currentStateMap.put(NODE_0, OFFLINE);
    _currentStateMap.put(NODE_1, SLAVE);
    _expectedBestPossibleMap.put(NODE_0, SLAVE);
    _expectedBestPossibleMap.put(NODE_1, MASTER);

    Map<String, String> bestPossibleMap =
        _rebalancer.computeBestPossibleStateForPartition(_stateModelDefinition, _preferenceList, _currentStateMap,
            _liveInstance, _disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, _expectedBestPossibleMap);
  }

  @Test
  public void case2() {
    // Be able to switch Master and Slave
    _currentStateMap.put(NODE_0, SLAVE);
    _currentStateMap.put(NODE_1, MASTER);
    _expectedBestPossibleMap.put(NODE_0, MASTER);
    _expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        _rebalancer.computeBestPossibleStateForPartition(_stateModelDefinition, _preferenceList, _currentStateMap,
            _liveInstance, _disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, _expectedBestPossibleMap);
  }

  @Test
  public void case3() {
    // Stay the same
    _currentStateMap.put(NODE_0, MASTER);
    _currentStateMap.put(NODE_1, SLAVE);
    _expectedBestPossibleMap.put(NODE_0, MASTER);
    _expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        _rebalancer.computeBestPossibleStateForPartition(_stateModelDefinition, _preferenceList, _currentStateMap,
            _liveInstance, _disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, _expectedBestPossibleMap);
  }

  @Test
  public void case4() {
    // Promote normally
    _currentStateMap.put(NODE_0, OFFLINE);
    _currentStateMap.put(NODE_1, OFFLINE);
    _expectedBestPossibleMap.put(NODE_0, MASTER);
    _expectedBestPossibleMap.put(NODE_1, SLAVE);

    Map<String, String> bestPossibleMap =
        _rebalancer.computeBestPossibleStateForPartition(_stateModelDefinition, _preferenceList, _currentStateMap,
            _liveInstance, _disabledInstances, true);
    Assert.assertEquals(bestPossibleMap, _expectedBestPossibleMap);
  }
}
