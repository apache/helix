package org.apache.helix.integration.task;

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

import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.task.TaskSynchronizedTestBase;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.tools.ClusterStateVerifier;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public class TaskTestBase extends TaskSynchronizedTestBase {
  protected ClusterControllerManager _controller;

  @BeforeClass
  public void beforeClass() throws Exception {
    String namespace = "/" + CLUSTER_NAME;
    if (_gZkClient.exists(namespace)) {
      _gZkClient.deleteRecursive(namespace);
    }

    _setupTool = new ClusterSetup(ZK_ADDR);
    _setupTool.addCluster(CLUSTER_NAME, true);
    setupParticipants();
    setupDBs();
    startParticipants();

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    createManagers();

    boolean result = ClusterStateVerifier.verifyByZkCallback(
        new ClusterStateVerifier.BestPossAndExtViewZkVerifier(ZK_ADDR, CLUSTER_NAME));
    Assert.assertTrue(result);
  }

  @AfterClass
  public void afterClass() throws Exception {
    super.afterClass();
    _controller.syncStop();
  }
}
