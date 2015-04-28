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

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.MockParticipant;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestParticipantNameCollision extends ZkStandAloneCMTestBase {
  private static Logger logger = Logger.getLogger(TestParticipantNameCollision.class);

  @Test()
  public void testParticiptantNameCollision() throws Exception {
    logger.info("RUN TestParticipantNameCollision() at " + new Date(System.currentTimeMillis()));

    MockParticipant newParticipant = null;
    for (int i = 0; i < 1; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      try {
        // the call fails on getClusterManagerForParticipant()
        // no threads start
        newParticipant = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);
        newParticipant.syncStart();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    Thread.sleep(30000);
    TestHelper.verifyWithTimeout("verifyNotConnected", 30 * 1000, newParticipant);

    logger.info("STOP TestParticipantNameCollision() at " + new Date(System.currentTimeMillis()));
  }
}
