package org.apache.helix.integration;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier;
import org.apache.helix.tools.ClusterVerifiers.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.util.GZipCompressionUtil;
import org.testng.Assert;
import org.testng.annotations.Test;

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
/**
 * Test controller, spectator and participant roles when compression is enabled.
 * Compression can be enabled for a specific resource by setting enableCompression=true in the
 * idealstate of the resource. Generally this is used when the number of partitions is large
 */
public class TestEnableCompression extends ZkIntegrationTestBase {
  @Test()
  public void testEnableCompressionResource() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    String className = TestHelper.getTestClassName();
    String methodName = TestHelper.getTestMethodName();
    String clusterName = className + "_" + methodName;

    System.out.println("START " + clusterName + " at " + new Date(System.currentTimeMillis()));

    MockParticipantManager[] participants = new MockParticipantManager[5];
    // ClusterSetup setupTool = new ClusterSetup(ZK_ADDR);
    int numNodes = 10;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        0, // no resources, will be added later
        0, // partitions per resource
        numNodes, // number of nodes
        0, // replicas
        "OnlineOffline", false); // dont rebalance
    List<String> instancesInCluster =
        _gSetupTool.getClusterManagementTool().getInstancesInCluster(clusterName);
    String resourceName = "TestResource";
    CustomModeISBuilder customModeISBuilder = new CustomModeISBuilder(resourceName);

    int numPartitions = 10000;
    int numReplica = 3;
    customModeISBuilder.setNumPartitions(numPartitions);
    customModeISBuilder.setNumReplica(numReplica);
    customModeISBuilder.setStateModel("OnlineOffline");
    for (int p = 0; p < numPartitions; p++) {
      String partitionName = resourceName + "_" + p;
      customModeISBuilder.add(partitionName);
      for (int r = 0; r < numReplica; r++) {
        String instanceName = instancesInCluster.get((p % numNodes + r) % numNodes);
        customModeISBuilder.assignInstanceAndState(partitionName, instanceName, "ONLINE");
      }
    }

    IdealState idealstate = customModeISBuilder.build();
    idealstate.getRecord().setBooleanField("enableCompression", true);
    _gSetupTool.getClusterManagementTool().addResource(clusterName, resourceName, idealstate);

    ZkClient zkClient =
        new ZkClient(ZK_ADDR, 60 * 1000, 60 * 1000, new BytesPushThroughSerializer());
    zkClient.waitUntilConnected(10, TimeUnit.SECONDS);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
            clusterName), 120000);
    Assert.assertTrue(result);

    List<String> compressedPaths = new ArrayList<String>();
    findCompressedZNodes(zkClient, "/", compressedPaths);

    System.out.println("compressed paths:" + compressedPaths);
    // ONLY IDEALSTATE and EXTERNAL VIEW must be compressed
    Assert.assertEquals(compressedPaths.size(), 2);
    String idealstatePath =
        PropertyPathBuilder.getPath(PropertyType.IDEALSTATES, clusterName, resourceName);
    String externalViewPath =
        PropertyPathBuilder.getPath(PropertyType.EXTERNALVIEW, clusterName, resourceName);
    Assert.assertTrue(compressedPaths.contains(idealstatePath));
    Assert.assertTrue(compressedPaths.contains(externalViewPath));

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void findCompressedZNodes(ZkClient zkClient, String path, List<String> compressedPaths) {
    List<String> children = zkClient.getChildren(path);
    if (children != null && children.size() > 0) {
      for (String child : children) {
        String childPath = (path.equals("/") ? "" : path) + "/" + child;
        findCompressedZNodes(zkClient, childPath, compressedPaths);
      }
    } else {
      byte[] data = zkClient.readData(path);
      if (data != null && GZipCompressionUtil.isCompressed(data)) {
        compressedPaths.add(path);
      }
    }

  }
}
