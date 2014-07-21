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

import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestDisableNode extends ZkStandAloneCMTestBase {

  @Test()
  public void testDisableNode() throws Exception {
    String command =
        "-zkSvr " + _zkaddr + " -enableInstance " + CLUSTER_NAME + " " + "localhost_12918"
            + " TestDB TestDB_0 false";
    ClusterSetup.processCommandLineArgs(command.split(" "));
    boolean result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

    ZKHelixAdmin tool = new ZKHelixAdmin(_zkclient);
    tool.enableInstance(CLUSTER_NAME, "localhost_12918", true);

    result =
        ClusterStateVerifier.verifyByPolling(new ClusterStateVerifier.BestPossAndExtViewZkVerifier(
            _zkaddr, CLUSTER_NAME));
    Assert.assertTrue(result);

  }
}
