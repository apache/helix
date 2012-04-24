/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.integration;

import java.util.Date;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.helix.TestHelper;
import com.linkedin.helix.tools.ClusterStateVerifier;

public class TestDynamicFileClusterManager extends FileCMTestBase
{
  private static Logger LOG = Logger.getLogger(TestDynamicFileClusterManager.class);

  @Test()
  public void testDynamicFileClusterManager()
  throws Exception
  {
    System.out.println("RUN testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));

    // add a new db
    _mgmtTool.addResource(CLUSTER_NAME, "MyDB", 6, STATE_MODEL);
    rebalanceStorageCluster(CLUSTER_NAME, "MyDB", 0);

    boolean result = ClusterStateVerifier.verify(
        new ClusterStateVerifier.BestPossAndExtViewFileVerifier(ROOT_PATH, CLUSTER_NAME));
    Assert.assertTrue(result);

    // drop db
    _mgmtTool.dropResource(CLUSTER_NAME, "MyDB");

    TestHelper.verifyWithTimeout("verifyEmptyCurStateFile",
                                 CLUSTER_NAME,
                                 "MyDB",
                                 TestHelper.<String>setOf("localhost_12918", "localhost_12919",
                                                          "localhost_12920", "localhost_12921",
                                                          "localhost_12922"),
                                                          _fileStore);

    System.out.println("STOP testDynamicFileClusterManager() at " + new Date(System.currentTimeMillis()));
  }

}
