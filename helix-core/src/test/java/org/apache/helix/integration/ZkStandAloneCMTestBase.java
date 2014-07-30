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

import org.apache.helix.manager.zk.MockParticipant;
import org.apache.helix.manager.zk.MockController;
import org.apache.helix.testutil.ZkTestBase;
import org.apache.helix.tools.ClusterStateVerifier;
import org.apache.helix.tools.ClusterStateVerifier.BestPossAndExtViewZkVerifier;
import org.apache.helix.tools.ClusterStateVerifier.MasterNbInExtViewVerifier;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

/**
 * setup a storage cluster and start a zk-based cluster controller in stand-alone mode
 * start 5 dummy participants verify the current states at end
 */

public class ZkStandAloneCMTestBase extends ZkTestBase {
  private static Logger LOG = Logger.getLogger(ZkStandAloneCMTestBase.class);

  protected static final int NODE_NR = 5;
  protected static final int START_PORT = 12918;
  protected static final String STATE_MODEL = "MasterSlave";
  protected static final String TEST_DB = "TestDB";
  protected static final int _PARTITIONS = 20;

  protected final String CLASS_NAME = this.getClass().getSimpleName();
  protected final String CLUSTER_NAME = CLASS_NAME;

  protected MockParticipant[] _participants = new MockParticipant[NODE_NR];
  protected MockController _controller;

  int _replica = 3;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Logger.getRootLogger().setLevel(Level.INFO);
    System.out.println("START " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));

    String namespace = "/" + CLUSTER_NAME;
    if (_zkclient.exists(namespace)) {
      _zkclient.deleteRecursive(namespace);
    }

    // setup storage cluster
    _setupTool.addCluster(CLUSTER_NAME, true);
    _setupTool.addResourceToCluster(CLUSTER_NAME, TEST_DB, _PARTITIONS, STATE_MODEL);
    for (int i = 0; i < NODE_NR; i++) {
      String storageNodeName = "localhost_" + (START_PORT + i);
      _setupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }
    _setupTool.rebalanceStorageCluster(CLUSTER_NAME, TEST_DB, _replica);

    // start dummy participants
    for (int i = 0; i < NODE_NR; i++) {
      String instanceName = "localhost_" + (START_PORT + i);
      _participants[i] = new MockParticipant(_zkaddr, CLUSTER_NAME, instanceName);
      _participants[i].syncStart();
    }

    // start controller
    String controllerName = "controller_0";
    _controller = new MockController(_zkaddr, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    boolean result =
        ClusterStateVerifier
            .verifyByZkCallback(new MasterNbInExtViewVerifier(_zkaddr, CLUSTER_NAME));

    result =
        ClusterStateVerifier.verifyByZkCallback(new BestPossAndExtViewZkVerifier(_zkaddr,
            CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    /**
     * shutdown order: 1) disconnect the controller 2) disconnect participants
     */

    _controller.syncStop();
    for (int i = 0; i < NODE_NR; i++) {
      _participants[i].syncStop();
    }

    System.out.println("END " + CLASS_NAME + " at " + new Date(System.currentTimeMillis()));
  }
}
