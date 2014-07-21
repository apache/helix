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

import org.apache.helix.testutil.ZkTestBase;
import org.testng.annotations.Test;

public class TestCMWithFailParticipant extends ZkTestBase {

  @Test()
  public void testCMWithFailParticipant() throws Exception {
    int numResources = 1;
    int numPartitionsPerResource = 10;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName =
        "TestFail_" + "rg" + numResources + "_p" + numPartitionsPerResource + "_n" + numInstance
            + "_r" + replica;
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupCluster(uniqClusterName, _zkaddr, numResources, numPartitionsPerResource,
        numInstance, replica);

    for (int i = 0; i < numInstance; i++) {
      TestDriver.startDummyParticipant(_zkaddr, uniqClusterName, i);
    }
    TestDriver.startController(_zkaddr, uniqClusterName);

    TestDriver.stopDummyParticipant(uniqClusterName, 2000, 0);
    TestDriver.verifyCluster(_zkaddr, uniqClusterName, 3000, 50 * 1000);
    TestDriver.stopCluster(uniqClusterName);

    System.out.println("END " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

  }
}
