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
import org.apache.helix.manager.zk.ZKUtil;
import org.apache.helix.testutil.ZkTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestZkCopy extends ZkTestBase {

  @Test
  public void test() throws Exception {
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));
    String fromPath = "/" + clusterName + "/from";
    _zkclient.createPersistent(fromPath, true);
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        String path = String.format("%s/%d/%d", fromPath, i, j);
        _zkclient.createPersistent(path, true);
        _zkclient.writeData(path, new ZNRecord(String.format("%d/%d", i, j)));
      }
    }

    // Copy
    String toPath = "/" + clusterName + "/to";
    ZkCopy.main(new String[]{"--src", "zk://" + _zkaddr + fromPath, "--dst", "zk://" + _zkaddr + toPath});

    // Verify
    Assert.assertTrue(_zkclient.exists(toPath));
    Assert.assertNull(_zkclient.readData(toPath));
    for (int i = 0; i < 5; i++) {
      for (int j = 0; j < 5; j++) {
        String path = String.format("%s/%d/%d", toPath, i, j);
        Assert.assertTrue(_zkclient.exists(path));
        ZNRecord record = _zkclient.readData(path);
        Assert.assertEquals(String.format("%d/%d", i, j), record.getId());
      }
    }

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

    TestHelper.setupCluster(srcClusterName, _zkaddr, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        1, // resources
        32, // partitions per resource
        n, // number of nodes
        2, // replicas
        "MasterSlave", true); // do rebalance

    TestHelper.setupEmptyCluster(_zkclient, dstClusterName);

    String fromPath = String.format("/%s/INSTANCES", srcClusterName);
    String toPath = String.format("/%s/INSTANCES", dstClusterName);
    ZkCopy.main(new String[] {
        "--src", "zk://" + _zkaddr + fromPath, "--dst", "zk://" + _zkaddr + toPath
    });

    fromPath = String.format("/%s/CONFIGS/PARTICIPANT", srcClusterName);
    toPath = String.format("/%s/CONFIGS/PARTICIPANT", dstClusterName);
    ZkCopy.main(new String[] {
        "--src", "zk://" + _zkaddr + fromPath, "--dst", "zk://" + _zkaddr + toPath
    });

    for (int i = 0; i < n; i++) {
      String instanceName = "localhost_" + (12918 + i);
      boolean ret =
          ZKUtil
              .isInstanceSetup(_zkclient, dstClusterName, instanceName, InstanceType.PARTICIPANT);
      Assert.assertTrue(ret);
    }
    System.out.println("END " + testName + " at " + new Date(System.currentTimeMillis()));
  }
}
