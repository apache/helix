package org.apache.helix.tools;

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

import org.apache.helix.InstanceType;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.ZkUnitTestBase;
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.tools.commandtools.ZkCopy;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCopy extends ZkUnitTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String fromPath = "/" + clusterName + "/from";
    _gZkClient.createPersistent(fromPath, true);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        String path = String.format("%s/%d/%d", fromPath, i, j);
        _gZkClient.createPersistent(path, true);
        _gZkClient.writeData(path, new ZNRecord(String.format("%d/%d", i, j)));
      }
    }

    // Copy
    String toPath = "/" + clusterName + "/to";
    ZkCopy.main(
        new String[] { "--src", "zk://" + ZK_ADDR + fromPath, "--dst", "zk://" + ZK_ADDR + toPath });

    // Verify
    Assert.assertTrue(_gZkClient.exists(toPath));
    Assert.assertNull(_gZkClient.readData(toPath));
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        String path = String.format("%s/%d/%d", toPath, i, j);
        Assert.assertTrue(_gZkClient.exists(path));
        ZNRecord record = _gZkClient.readData(path);
        Assert.assertEquals(String.format("%d/%d", i, j), record.getId());
      }
    }

    _gZkClient.deleteRecursively("/" + clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  @Test
  public void testSkipCopyExistZnode() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String testName = className + "_" + methodName;

    System.out.println("START " + testName + " at " + new Date(System.currentTimeMillis()));
    String srcClusterName = testName + "_src";
    String dstClusterName = testName + "_dst";
    int n = 5;

    TestHelper.setupCluster(srcClusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    TestHelper.setupEmptyCluster(_gZkClient, dstClusterName);

    String fromPath = String.format("/%s/INSTANCES", srcClusterName);
    String toPath = String.format("/%s/INSTANCES", dstClusterName);
    ZkCopy.main(new String[] {
        "--src", "zk://" + ZK_ADDR + fromPath, "--dst", "zk://" + ZK_ADDR + toPath
    });

    fromPath = String.format("/%s/CONFIGS/PARTICIPANT", srcClusterName);
    toPath = String.format("/%s/CONFIGS/PARTICIPANT", dstClusterName);
    ZkCopy.main(new String[] {
        "--src", "zk://" + ZK_ADDR + fromPath, "--dst", "zk://" + ZK_ADDR + toPath
    });

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      boolean ret =
          ZKUtil
              .isInstanceSetup(_gZkClient, dstClusterName, instanceName, InstanceType.PARTICIPANT);
      Assert.assertTrue(ret);
    }

    TestHelper.dropCluster(srcClusterName, _gZkClient);
    TestHelper.dropCluster(dstClusterName, _gZkClient);
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
}
