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

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.TestHelper;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisablePartition extends ZkStandAloneCMTestBase {
  private static Logger LOG = Logger.getLogger(TestDisablePartition.class);

  @Test()
  public void testDisablePartition() throws Exception {
    LOG.info("START testDisablePartition() at " + new Date(System.currentTimeMillis()));

    // localhost_12919 is MASTER for TestDB_0
    String command =
        "--zkSvr " + _zkaddr + " --enablePartition false " + CLUSTER_NAME
            + " localhost_12919 TestDB TestDB_0 TestDB_9";
    ClusterSetup.processCommandLineArgs(command.split("\\s+"));
    Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    map.put("TestDB_0", TestHelper.setOf("localhost_12919"));
    map.put("TestDB_9", TestHelper.setOf("localhost_12919"));

    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    TestHelper.verifyState(CLUSTER_NAME, _zkaddr, map, "OFFLINE");

    ZKHelixAdmin tool = new ZKHelixAdmin(_zkclient);
    tool.enablePartition(true, CLUSTER_NAME, "localhost_12919", "TestDB", Arrays.asList("TestDB_9"));

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    map.clear();
    map.put("TestDB_0", TestHelper.setOf("localhost_12919"));
    TestHelper.verifyState(CLUSTER_NAME, _zkaddr, map, "OFFLINE");

    map.clear();
    map.put("TestDB_9", TestHelper.setOf("localhost_12919"));
    TestHelper.verifyState(CLUSTER_NAME, _zkaddr, map, "MASTER");

    LOG.info("STOP testDisablePartition() at " + new Date(System.currentTimeMillis()));

  }

}
