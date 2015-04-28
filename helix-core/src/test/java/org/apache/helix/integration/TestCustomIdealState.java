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
import org.apache.helix.testutil.ZkTestBase;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

public class TestCustomIdealState extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(TestCustomIdealState.class);

  @Test
  public void testBasic() throws Exception {

    int numResources = 2;
    int numPartitionsPerResource = 100;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName =
        "TestCustomIS_" + "rg" + numResources + "_p" + numPartitionsPerResource + "_n"
            + numInstance + "_r" + replica + "_basic";
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqClusterName, _zkaddr, numResources,
        numPartitionsPerResource, numInstance, replica);

    for (int i = 0; i < numInstance; i++) {
      TestDriver.startDummyParticipant(_zkaddr, uniqClusterName, i);
    }
    TestDriver.startController(_zkaddr, uniqClusterName);

    TestDriver.setIdealState(_zkaddr, uniqClusterName, 2000, 50);
    TestDriver.verifyCluster(_zkaddr, uniqClusterName, 3000, 50 * 1000);

    TestDriver.stopCluster(uniqClusterName);

    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testNonAliveInstances() throws Exception {
    int numResources = 2;
    int numPartitionsPerResource = 50;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName =
        "TestCustomIS_" + "rg" + numResources + "_p" + numPartitionsPerResource + "_n"
            + numInstance + "_r" + replica + "_nonalive";
    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

    TestDriver.setupClusterWithoutRebalance(uniqClusterName, _zkaddr, numResources,
        numPartitionsPerResource, numInstance, replica);

    for (int i = 0; i < numInstance / 2; i++) {
      TestDriver.startDummyParticipant(_zkaddr, uniqClusterName, i);
    }

    TestDriver.startController(_zkaddr, uniqClusterName);
    TestDriver.setIdealState(_zkaddr, uniqClusterName, 0, 100);

    // wait some time for customized ideal state being populated
    Thread.sleep(1000);

    // start the rest of participants after ideal state is set
    for (int i = numInstance / 2; i < numInstance; i++) {
      TestDriver.startDummyParticipant(_zkaddr, uniqClusterName, i);
    }

    TestDriver.verifyCluster(_zkaddr, uniqClusterName, 4000, 50 * 1000);

    TestDriver.stopCluster(uniqClusterName);

    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));

  }

  @Test()
  public void testDrop() throws Exception {
    int numResources = 2;
    int numPartitionsPerResource = 50;
    int numInstance = 5;
    int replica = 3;

    String uniqClusterName =
        "TestCustomIS_" + "rg" + numResources + "_p" + numPartitionsPerResource + "_n"
            + numInstance + "_r" + replica + "_drop";

    System.out.println("START " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
    TestDriver.setupClusterWithoutRebalance(uniqClusterName, _zkaddr, numResources,
        numPartitionsPerResource, numInstance, replica);

    for (int i = 0; i < numInstance; i++) {
      TestDriver.startDummyParticipant(_zkaddr, uniqClusterName, i);
    }
    TestDriver.startController(_zkaddr, uniqClusterName);
    TestDriver.setIdealState(_zkaddr, uniqClusterName, 2000, 50);
    TestDriver.verifyCluster(_zkaddr, uniqClusterName, 3000, 50 * 1000);

    // drop resource group
    _setupTool.dropResourceFromCluster(uniqClusterName, "TestDB0");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateAndExtView", 30 * 1000, uniqClusterName,
        "TestDB0", TestHelper.<String> setOf("localhost_12918", "localhost_12919",
            "localhost_12920", "localhost_12921", "localhost_12922"), _zkaddr);

    TestDriver.stopCluster(uniqClusterName);
    System.out.println("STOP " + uniqClusterName + " at " + new Date(System.currentTimeMillis()));
  }

  // TODO add a test case that verify (in case of node failure) best possible
  // state is a subset of ideal state
}
