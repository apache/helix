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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.util.GZipCompressionUtil;
import org.apache.helix.zookeeper.api.client.HelixZkClient;
import org.apache.helix.zookeeper.impl.factory.SharedZkClientFactory;
import org.apache.helix.zookeeper.zkclient.serialize.BytesPushThroughSerializer;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test controller, spectator and participant roles when compression is enabled.
 * Compression can be enabled for a specific resource by setting enableCompression=true in the
 * idealstate of the resource. Generally this is used when the number of partitions is large
 */
public class TestEnableCompression extends ZkTestBase {
  @Test()
  public void testEnableCompressionResource() throws Exception {
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
    Set<String> expectedResources = new HashSet<>();
    expectedResources.add(resourceName);
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

    HelixZkClient.ZkClientConfig clientConfig = new HelixZkClient.ZkClientConfig();
    clientConfig.setZkSerializer(new BytesPushThroughSerializer())
        .setOperationRetryTimeout((long) (60 * 1000)).setConnectInitTimeout(60 * 1000);
    HelixZkClient zkClient = SharedZkClientFactory.getInstance()
        .buildZkClient(new HelixZkClient.ZkConnectionConfig(ZK_ADDR), clientConfig);

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    Set<String> expectedLiveInstances = new HashSet<>();
    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);
      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
      expectedLiveInstances.add(instanceName);
    }

    BestPossibleExternalViewVerifier verifier =
        new BestPossibleExternalViewVerifier.Builder(clusterName).setZkAddr(ZK_ADDR)
            .setExpectLiveInstances(expectedLiveInstances).setResources(expectedResources).build();
    boolean result = verifier.verify(120000L);
    Assert.assertTrue(result);

    List<String> compressedPaths = new ArrayList<>();
    findCompressedZNodes(zkClient, "/" + clusterName, compressedPaths);

    System.out.println("compressed paths:" + compressedPaths);
    // ONLY IDEALSTATE and EXTERNAL VIEW must be compressed
    Assert.assertEquals(compressedPaths.size(), 2);
    String idealstatePath = PropertyPathBuilder.idealState(clusterName, resourceName);
    String externalViewPath = PropertyPathBuilder.externalView(clusterName, resourceName);
    Assert.assertTrue(compressedPaths.contains(idealstatePath));
    Assert.assertTrue(compressedPaths.contains(externalViewPath));

    // clean up
    controller.syncStop();
    for (int i = 0; i < 5; i++) {
      participants[i].syncStop();
    }

    deleteCluster(clusterName);
    System.out.println("END " + clusterName + " at " + new Date(System.currentTimeMillis()));
  }

  private void findCompressedZNodes(HelixZkClient zkClient, String path,
      List<String> compressedPaths) {
    List<String> children = zkClient.getChildren(path);
    if (children != null && children.size() > 0) {
      for (String child : children) {
        String childPath = (path.equals("/") ? "" : path) + "/" + child;
        findCompressedZNodes(zkClient, childPath, compressedPaths);
      }
    } else {
      byte[] data = zkClient.readData(path);
      if (GZipCompressionUtil.isCompressed(data)) {
        compressedPaths.add(path);
      }
    }
  }
}
