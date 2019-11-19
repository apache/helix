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

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.common.ZkStandAloneCMTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterStateVerifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStandAloneCMMain extends ZkStandAloneCMTestBase {
  private static Logger logger = LoggerFactory.getLogger(TestStandAloneCMMain.class);

  @Test()
  public void testStandAloneCMMain() throws Exception {
    logger.info("RUN testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));

    // Keep references to the controllers created so that they could be shut down
    Set<ClusterControllerManager> controllers = new HashSet<>();

    ClusterControllerManager newController = null;
    for (int i = 1; i <= 2; i++) {
      String controllerName = "controller_" + i;
      newController = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
      newController.syncStart();
      controllers.add(newController);
    }

    _controller.syncStop();

    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final String newControllerName = newController.getInstanceName();
    TestHelper.verify(() -> {
      LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
      if (leader == null) {
        return false;
      }
      return leader.getInstanceName().equals(newControllerName);

    }, 30 * 1000);

    Assert.assertTrue(ClusterStateVerifier.verifyByPolling(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME)));

    // Shut down all controllers so that the cluster could be deleted
    for (ClusterControllerManager controller : controllers) {
      controller.syncStop();
    }

    logger.info("STOP testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
  }
}
