package org.apache.helix.manager.zk;

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

import org.apache.helix.integration.ZkStandAloneCMTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestLiveInstanceBounce extends ZkStandAloneCMTestBase {
  @Test
  public void testInstanceBounce() throws Exception {
    int handlerSize = _controller.getHandlers().size();

    for (int i = 0; i < 2; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      // kill 2 participants
      _participants[i].syncStop();
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      // restart the participant
      _participants[i] = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
      Thread.sleep(100);
    }
    Thread.sleep(4000);

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME), 50 * 1000);
    Assert.assertTrue(result);

    // When a new live instance is created, we add current state listener to it
    // and we will remove current-state listener on expired session
    // so the number of callback handlers is unchanged
    for (int j = 0; j < 10; j++) {
      if (_controller.getHandlers().size() == (handlerSize)) {
        break;
      }
      Thread.sleep(400);
    }
    Assert.assertEquals(_controller.getHandlers().size(), handlerSize);
  }
}
