package org.apache.helix.integration;

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
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.model.LeaderHistory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class TestControllerHistory extends ZkStandAloneCMTestBase {

  @Test()
  public void testControllerLeaderHistory() throws Exception {
    HelixManager manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    manager.connect();

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(CLUSTER_NAME);
    PropertyKey propertyKey = keyBuilder.controllerLeaderHistory();
    LeaderHistory leaderHistory = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNotNull(leaderHistory);
    List<String> list = leaderHistory.getRecord().getListField("HISTORY");
    Assert.assertEquals(list.size(), 1);

    for (int i = 0; i <= 12; i++) {
      _controller.syncStop();
      _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, "Controller-" + i);
      _controller.syncStart();
    }

    leaderHistory = manager.getHelixDataAccessor().getProperty(propertyKey);
    Assert.assertNotNull(leaderHistory);
    list = leaderHistory.getRecord().getListField("HISTORY");
    Assert.assertEquals(list.size(), 10);
    manager.disconnect();
  }
}
