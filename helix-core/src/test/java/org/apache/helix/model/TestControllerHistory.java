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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.helix.TestHelper;
import org.apache.helix.api.status.ClusterManagementMode;
import org.apache.helix.util.HelixUtil;
import org.apache.helix.zookeeper.zkclient.NetworkUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestControllerHistory {
  @Test
  public void testManagementModeHistory() {
    ControllerHistory controllerHistory = new ControllerHistory("HISTORY");
    String controller = "controller-0";
    ClusterManagementMode mode = new ClusterManagementMode(ClusterManagementMode.Type.CLUSTER_PAUSE,
        ClusterManagementMode.Status.COMPLETED);
    long time = System.currentTimeMillis();
    String fromHost = NetworkUtil.getLocalhostName();
    String reason = TestHelper.getTestMethodName();
    controllerHistory.updateManagementModeHistory(controller, mode, fromHost, time, reason);

    List<String> historyList = controllerHistory.getManagementModeHistory();
    String lastHistory = historyList.get(historyList.size() - 1);
    Map<String, String> historyMap = HelixUtil.stringToMap(lastHistory);

    Map<String, String> expectedMap = new HashMap<>();
    expectedMap.put("CONTROLLER", controller);
    expectedMap.put("TIME", Instant.ofEpochMilli(time).toString());
    expectedMap.put("MODE", mode.getMode().name());
    expectedMap.put("STATUS", mode.getStatus().name());
    expectedMap.put(PauseSignal.PauseSignalProperty.FROM_HOST.name(), fromHost);
    expectedMap.put(PauseSignal.PauseSignalProperty.REASON.name(), reason);

    Assert.assertEquals(historyMap, expectedMap);

    // Add more than 10 entries, it should only keep the latest 10.
    for (int i = 0; i < 15; i++) {
      controllerHistory.updateManagementModeHistory(controller, mode, fromHost, time, reason);
    }

    historyList = controllerHistory.getManagementModeHistory();

    Assert.assertEquals(historyList.size(), 10);
  }
}
