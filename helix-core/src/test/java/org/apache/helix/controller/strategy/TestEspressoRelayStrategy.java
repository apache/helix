package org.apache.helix.controller.strategy;

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
import java.util.TreeMap;

import org.apache.helix.model.IdealState;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestEspressoRelayStrategy {
  @Test()
  public void testEspressoStorageClusterIdealState() throws Exception {
    testEspressoStorageClusterIdealState(15, 9, 3);
    testEspressoStorageClusterIdealState(15, 6, 3);
    testEspressoStorageClusterIdealState(15, 6, 2);
    testEspressoStorageClusterIdealState(6, 4, 2);
  }

  public void testEspressoStorageClusterIdealState(int partitions, int nodes, int replica)
      throws Exception {
    List<String> storageNodes = new ArrayList<String>();
    for (int i = 0; i < partitions; i++) {
      storageNodes.add("localhost:123" + i);
    }

    List<String> relays = new ArrayList<String>();
    for (int i = 0; i < nodes; i++) {
      relays.add("relay:123" + i);
    }

    IdealState idealstate =
        EspressoRelayStrategy.calculateRelayIdealState(storageNodes, relays, "TEST", replica,
            "Leader", "Standby", "LeaderStandby");

    Assert.assertEquals(idealstate.getRecord().getListFields().size(), idealstate.getRecord()
        .getMapFields().size());

    Map<String, Integer> countMap = new TreeMap<String, Integer>();
    for (String key : idealstate.getRecord().getListFields().keySet()) {
      Assert.assertEquals(idealstate.getRecord().getListFields().get(key).size(), idealstate
          .getRecord().getMapFields().get(key).size());
      List<String> list = idealstate.getRecord().getListFields().get(key);
      Map<String, String> map = idealstate.getRecord().getMapFields().get(key);
      Assert.assertEquals(list.size(), replica);
      for (String val : list) {
        if (!countMap.containsKey(val)) {
          countMap.put(val, 1);
        } else {
          countMap.put(val, countMap.get(val) + 1);
        }
        Assert.assertTrue(map.containsKey(val));
      }
    }
    for (String nodeName : countMap.keySet()) {
      Assert.assertTrue(countMap.get(nodeName) <= partitions * replica / nodes + 1);
      // System.out.println(nodeName + " " + countMap.get(nodeName));
    }
    System.out.println();
  }
}
