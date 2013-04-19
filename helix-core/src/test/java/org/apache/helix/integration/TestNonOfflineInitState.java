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
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.mock.participant.MockParticipant;
import org.apache.helix.mock.participant.MockBootstrapModelFactory;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestNonOfflineInitState extends ZkIntegrationTestBase
{
  private static Logger LOG = Logger.getLogger(TestNonOfflineInitState.class);

  @Test
  public void testNonOfflineInitState() throws Exception
  {
    System.out.println("START testNonOfflineInitState at "
        + new Date(System.currentTimeMillis()));
    String clusterName = getShortClassName();

    setupCluster(clusterName, ZK_ADDR, 12918, // participant port
                 "localhost", // participant name prefix
                 "TestDB", // resource name prefix
                 1, // resources
                 10, // partitions per resource
                 5, // number of nodes
                 1, // replicas
                 "Bootstrap",
                 true); // do rebalance

    TestHelper.startController(clusterName,
                               "controller_0",
                               ZK_ADDR,
                               HelixControllerMain.STANDALONE);

    // start participants
    MockParticipant[] participants = new MockParticipant[5];
    for (int i = 0; i < 5; i++)
    {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipant(clusterName, instanceName, ZK_ADDR, null);

      // add a state model with non-OFFLINE initial state
      StateMachineEngine stateMach = participants[i].getManager().getStateMachineEngine();
      MockBootstrapModelFactory bootstrapFactory = new MockBootstrapModelFactory();
      stateMach.registerStateModelFactory("Bootstrap", bootstrapFactory);

      participants[i].syncStart();
    }

    boolean result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(ZK_ADDR,
                                                                                 clusterName));
    Assert.assertTrue(result);

    System.out.println("END testNonOfflineInitState at "
        + new Date(System.currentTimeMillis()));
  }

  private static void setupCluster(String clusterName,
                                   String ZkAddr,
                                   int startPort,
                                   String participantNamePrefix,
                                   String resourceNamePrefix,
                                   int resourceNb,
                                   int partitionNb,
                                   int nodesNb,
                                   int replica,
                                   String stateModelDef,
                                   boolean doRebalance) throws Exception
  {
    ZkClient zkClient = new ZkClient(ZkAddr);
    if (zkClient.exists("/" + clusterName))
    {
      LOG.warn("Cluster already exists:" + clusterName + ". Deleting it");
      zkClient.deleteRecursive("/" + clusterName);
    }

    ClusterSetup setupTool = new ClusterSetup(ZkAddr);
    setupTool.addCluster(clusterName, true);
    setupTool.addStateModelDef(clusterName,
                               "Bootstrap",
                               TestHelper.generateStateModelDefForBootstrap());

    for (int i = 0; i < nodesNb; i++)
    {
      int port = startPort + i;
      setupTool.addInstanceToCluster(clusterName, participantNamePrefix + "_" + port);
    }

    for (int i = 0; i < resourceNb; i++)
    {
      String dbName = resourceNamePrefix + i;
      setupTool.addResourceToCluster(clusterName, dbName, partitionNb, stateModelDef);
      if (doRebalance)
      {
        setupTool.rebalanceStorageCluster(clusterName, dbName, replica);
      }
    }
    zkClient.close();
  }

}
