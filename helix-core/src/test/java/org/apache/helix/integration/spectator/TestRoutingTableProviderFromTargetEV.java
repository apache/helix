package org.apache.helix.integration.spectator;

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

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.integration.task.WorkflowGenerator;
import org.apache.helix.mock.participant.MockDelayMSStateModelFactory;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.MasterSlaveSMD;
import org.apache.helix.model.Message;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProviderFromTargetEV extends ZkTestBase {
  private static Logger LOG = LoggerFactory.getLogger(TestRoutingTableProviderFromTargetEV.class);
  private HelixManager _manager;
  private final String MASTER_SLAVE_STATE_MODEL = "MasterSlave";
  private final int NUM_NODES = 10;
  protected int NUM_PARTITIONS = 20;
  protected int NUM_REPLICAS = 3;
  private final int START_PORT = 12918;
  private final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + getShortClassName();
  private MockParticipantManager[] _participants;
  private ClusterControllerManager _controller;
  private ConfigAccessor _configAccessor;

  @BeforeClass
  public void beforeClass() throws Exception {
    _participants =  new MockParticipantManager[NUM_NODES];
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    _participants = new MockParticipantManager[NUM_NODES];
    for (int i = 0; i < NUM_NODES; i++) {
      String storageNodeName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, storageNodeName);
    }

    _gSetupTool.addResourceToCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_PARTITIONS,
        MASTER_SLAVE_STATE_MODEL, IdealState.RebalanceMode.FULL_AUTO.name(),
        CrushEdRebalanceStrategy.class.getName());

    _gSetupTool
        .rebalanceStorageCluster(CLUSTER_NAME, WorkflowGenerator.DEFAULT_TGT_DB, NUM_REPLICAS);

    for (int i = 0; i < NUM_NODES; i++) {
      String instanceName = PARTICIPANT_PREFIX + "_" + (START_PORT + i);
      _participants[i] = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);

      // add a delayed state model
      StateMachineEngine stateMachine = _participants[i].getStateMachineEngine();
      MockDelayMSStateModelFactory delayFactory =
          new MockDelayMSStateModelFactory().setDelay(-300000L);
      stateMachine.registerStateModelFactory(MASTER_SLAVE_STATE_MODEL, delayFactory);
      _participants[i].syncStart();
    }
    _manager = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "Admin", InstanceType.ADMINISTRATOR, ZK_ADDR);
    _manager.connect();

    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();
    _configAccessor = new ConfigAccessor(_gZkClient);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (_controller != null && _controller.isConnected()) {
      _controller.syncStop();
    }
    for (int i = 0; i < NUM_NODES; i++) {
      if (_participants[i] != null && _participants[i].isConnected()) {
        _participants[i].syncStop();
      }
    }
    if (_manager != null && _manager.isConnected()) {
      _manager.disconnect();
    }
    deleteCluster(CLUSTER_NAME);
  }

  @Test(expectedExceptions = HelixException.class)
  public void testTargetExternalViewWithoutEnable() {
    new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW);
  }

  @Test (dependsOnMethods = "testTargetExternalViewWithoutEnable")
  public void testExternalViewDoesNotExist() {
    String resourceName = WorkflowGenerator.DEFAULT_TGT_DB + 1;
    RoutingTableProvider externalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    try {
      Assert.assertEquals(externalViewProvider.getInstancesForResource(resourceName, "SLAVE").size(),
          0);
    } finally {
      externalViewProvider.shutdown();
    }
  }

  @Test (dependsOnMethods = "testExternalViewDoesNotExist")
  public void testExternalViewDiffFromTargetExternalView() throws Exception {
    ClusterConfig clusterConfig = _configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.enableTargetExternalView(true);
    clusterConfig.setPersistBestPossibleAssignment(true);
    _configAccessor.setClusterConfig(CLUSTER_NAME, clusterConfig);
    Thread.sleep(2000);

    RoutingTableProvider externalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.EXTERNALVIEW);
    final RoutingTableProvider targetExternalViewProvider =
        new RoutingTableProvider(_manager, PropertyType.TARGETEXTERNALVIEW);

    try {
      // If there is a pending message. TEV reflect to its target state and EV reflect to its current state.
      Set<InstanceConfig> externalViewMasters =
          externalViewProvider.getInstancesForResource(WorkflowGenerator.DEFAULT_TGT_DB, "MASTER");
      Assert.assertEquals(externalViewMasters.size(), 0);
      HelixDataAccessor accessor = _manager.getHelixDataAccessor();
      Assert.assertTrue(TestHelper.verify(() -> {
        ExternalView ev = accessor.getProperty(accessor.keyBuilder().externalView(WorkflowGenerator.DEFAULT_TGT_DB));
        ExternalView tev =
            accessor.getProperty(accessor.keyBuilder().targetExternalView(WorkflowGenerator.DEFAULT_TGT_DB));
        AtomicBoolean valid = new AtomicBoolean(true);
        List<String> instances = accessor.getChildNames(accessor.keyBuilder().instanceConfigs());
        instances.stream().forEach(i -> {
          List<Message> messages = accessor.getChildValues(accessor.keyBuilder().messages(i));
          messages.stream().forEach(m -> {
            if (!m.getFromState().equals(ev.getStateMap(m.getPartitionName()).get(i)) || !m.getToState()
                .equals(tev.getStateMap(m.getPartitionName()).get(i))) {
              valid.set(false);
            }
          });
        });
        return valid.get();
      }, 3000));
    } finally {
      externalViewProvider.shutdown();
      targetExternalViewProvider.shutdown();
    }
  }
}
