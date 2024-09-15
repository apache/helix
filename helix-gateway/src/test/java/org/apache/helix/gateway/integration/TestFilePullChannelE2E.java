package org.apache.helix.gateway.integration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.gateway.base.HelixGatewayTestBase;
import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.apache.helix.gateway.channel.GatewayServiceChannelConfig.ChannelMode.*;


public class TestFilePullChannelE2E extends HelixGatewayTestBase {

  private static final String CLUSTER_NAME = "CLUSTER_" + TestFilePullChannelE2E.class.getSimpleName();
  private static final int START_NUM_NODE = 3;
  private static final String TEST_DB = "TestDB";
  private static final String TEST_STATE_MODEL = "OnlineOffline";
  private static final String CONTROLLER_PREFIX = "controller";
  private static final String currentStatePath = "tmpcurrentState";
  private static final String targetStatePath = "tmptargetState";
  GatewayServiceManager manager1, manager2, manager0;
  ArrayList<Path> csPaths = new ArrayList<Path>();
  ArrayList<Path> targetPaths = new ArrayList<Path>();
  ArrayList<Path> healthPaths = new ArrayList<Path>();
  private ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() {
    super.beforeClass();

    // Set up the Helix cluster
    ConfigAccessor configAccessor = new ConfigAccessor(_gZkClient);
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord().setSimpleField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, "true");
    configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);

    // Start the controller
    String controllerName = CONTROLLER_PREFIX + '_' + CLUSTER_NAME;
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // Enable best possible assignment persistence
    enablePersistBestPossibleAssignment(_gZkClient, CLUSTER_NAME, true);
  }

  @Test
  public void testE2E() throws Exception {
    // create files for health state
    try {
      for (int i = 0; i < START_NUM_NODE; i++) {
        csPaths.add(createTempFile(currentStatePath + i, ".txt", ""));
        targetPaths.add(createTempFile(targetStatePath + i, ".txt", ""));
        String currentTime = String.valueOf(System.currentTimeMillis());
        String content = "{\"IsAlive\":" + true + ",\"LastUpdateTime\":" + currentTime + "}";
        healthPaths.add(createTempFile("tmphealthCheck" + i, ".txt", content));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    String fileName0 = healthPaths.get(0).toAbsolutePath().toString();
    String fileName1 = healthPaths.get(1).toAbsolutePath().toString();
    String fileName2 = healthPaths.get(2).toAbsolutePath().toString();

    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder().setChannelMode(POLL_MODE)
            .setParticipantConnectionChannelType(GatewayServiceChannelConfig.ChannelType.FILE)
            .setShardStateProcessorType(GatewayServiceChannelConfig.ChannelType.FILE)
            .setPollIntervalSec(1) // set a larger number to avoid recurrent polling
            .setPollStartDelaySec(1)
            .setTargetFileUpdateIntervalSec(1);

    // create empty file for shard state

    // create 3 manager instances
    manager0 = new GatewayServiceManager(ZK_ADDR,
        builder.addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.PARTICIPANT_CURRENT_STATE_PATH,
                csPaths.get(0).toAbsolutePath().toString())
            .addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.SHARD_TARGET_STATE_PATH,
                targetPaths.get(0).toAbsolutePath().toString())
            .setHealthCheckEndpointMap(Map.of(CLUSTER_NAME, Map.of("instance0", fileName0)))
            .build());
    manager1 = new GatewayServiceManager(ZK_ADDR,
        builder.addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.PARTICIPANT_CURRENT_STATE_PATH,
                csPaths.get(1).toAbsolutePath().toString())
            .addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.SHARD_TARGET_STATE_PATH,
                targetPaths.get(1).toAbsolutePath().toString())
            .setHealthCheckEndpointMap(Map.of(CLUSTER_NAME, Map.of("instance1", fileName1)))
            .build());
    manager2 = new GatewayServiceManager(ZK_ADDR,
        builder.addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.PARTICIPANT_CURRENT_STATE_PATH,
                csPaths.get(2).toAbsolutePath().toString())
            .addPollModeConfig(GatewayServiceChannelConfig.FileBasedConfigType.SHARD_TARGET_STATE_PATH,
                targetPaths.get(2).toAbsolutePath().toString())
            .setHealthCheckEndpointMap(Map.of(CLUSTER_NAME, Map.of("instance2", fileName2)))
            .build());

    System.out.println("Starting all managers");
    manager0.startService();
    manager1.startService();
    manager2.startService();

    // verify we see live instances
    verifyInstances(CLUSTER_NAME, List.of("instance0", "instance1", "instance2"));

    // create an DB on cluster
    createDB();

    // read the target state file and verify the target state is updated
    verifyTargetState();

    // write current state to file
    for (int i = 0; i < 3; i++) {
      String content =
          "{\"" + CLUSTER_NAME + "\" : { \"instance" + i + "\" : { \"TestDB\" : {\"TestDB_0\" : \"ONLINE\" }}}} ";
      Files.write(csPaths.get(i), content.getBytes());
    }

    // check no pending messages for partitions
    verifyNoPendingMessages(List.of("instance0", "instance1", "instance2"));

    // change health state to false on one instance
    String currentTime = String.valueOf(System.currentTimeMillis());
    String content = "{\"IsAlive\":" + false + ",\"LastUpdateTime\":" + currentTime + "}";
    Files.write(healthPaths.get(0), content.getBytes());

    // check live instance for that instance is gone
    Assert.assertTrue(TestHelper.verify(() -> {
      List<String> liveInstance = getLiveInstances();
      return !liveInstance.contains("instance0") && liveInstance.contains("instance1") && liveInstance.contains(
          "instance2");
    }, TestHelper.WAIT_DURATION));

    // stop all manager
    manager0.stopService();
    manager1.stopService();
    manager2.stopService();


    // check target state files are gone
    for (int i = 0; i < 3; i++) {
      Assert.assertFalse(Files.exists(targetPaths.get(i)));
    }

    // check all live instances are gone
    Assert.assertTrue(TestHelper.verify(() -> {
      List<String> liveInstance = getLiveInstances();
      return !liveInstance.contains("instance0") && !liveInstance.contains("instance1") && !liveInstance.contains(
          "instance2");
    }, TestHelper.WAIT_DURATION));

    for (int i = 0; i < 3; i++) {
      try {
        Files.deleteIfExists(csPaths.get(i));
        Files.deleteIfExists(targetPaths.get(i));
        Files.deleteIfExists(healthPaths.get(i));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }


  private void verifyInstances(String clusterName, List<String> instance0) throws Exception {
    for (String instance : instance0) {
      Assert.assertTrue(TestHelper.verify(
          () -> _gSetupTool.getClusterManagementTool().getInstancesInCluster(clusterName).contains(instance),
          TestHelper.WAIT_DURATION));
    }
  }

  private List<String> getLiveInstances() {
    ZKHelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    PropertyKey liveInstances = dataAccessor.keyBuilder().liveInstances();
    return dataAccessor.getChildNames(liveInstances);
  }

  private void verifyNoPendingMessages(List<String> participants) throws Exception {
    ZKHelixDataAccessor dataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, new ZkBaseDataAccessor<>(_gZkClient));
    for (String participant : participants) {
      PropertyKey messagesNode = dataAccessor.keyBuilder().messages(participant);
      Assert.assertTrue(
          TestHelper.verify(() -> dataAccessor.getChildNames(messagesNode).isEmpty(), TestHelper.WAIT_DURATION));
    }
  }

  private void verifyTargetState() throws Exception {
    for (int i = 0; i < 3; i++) {
      int finalI = i;
      Assert.assertTrue(TestHelper.verify(() -> {
        String content = Files.readString(targetPaths.get(finalI));
        return content.contains("{\"TestDB\":{\"TestDB_0\":\"ONLINE\"}}}");
      }, TestHelper.WAIT_DURATION));
    }
  }

  public static Path createTempFile(String prefix, String suffix, String content) throws IOException {
    // Create a temporary file
    Path tempFile = Files.createTempFile(prefix, suffix);

    // Write content to the temporary file
    Files.write(tempFile, content.getBytes());

    return tempFile;
  }

  private void createDB() {
    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, TEST_DB, List.of("instance0", "instance1", "instance2"),
        TEST_STATE_MODEL, 1, 3);

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkAddr(ZK_ADDR)
        .setResources(new HashSet<>(_gSetupTool.getClusterManagementTool().getResourcesInCluster(CLUSTER_NAME)))
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
  }
}
