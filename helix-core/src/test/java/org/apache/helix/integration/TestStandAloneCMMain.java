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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.TestHelper.Verifier;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestStandAloneCMMain extends ZkStandAloneCMTestBase {
  private static Logger logger = Logger.getLogger(TestStandAloneCMMain.class);

  @Test()
  public void testStandAloneCMMain() throws Exception {
    logger.info("RUN testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
    MockController newController = null;
    for (int i = 1; i <= 2; i++) {
      String controllerName = "controller_" + i;
      newController = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
      newController.syncStart();
    }

    // stopCurrentLeader(_zkClient, CLUSTER_NAME, _startCMResultMap);
    _controller.syncStop();

    final HelixDataAccessor accessor =
        new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    final String newControllerName = newController.getInstanceName();
    TestHelper.verify(new Verifier() {

      @Override
      public boolean verify() throws Exception {
        LiveInstance leader = accessor.getProperty(keyBuilder.controllerLeader());
        if (leader == null) {
          return false;
        }
        return leader.getInstanceName().equals(newControllerName);

      }
    }, 30 * 1000);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    logger.info("STOP testStandAloneCMMain() at " + new Date(System.currentTimeMillis()));
  }

}
