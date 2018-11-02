package org.apache.helix.view.integration;

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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.api.config.ViewClusterSourceConfig;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.view.mock.MockViewClusterSpectator;
import org.apache.helix.view.statemodel.DistViewAggregatorStateModel;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHelixViewAggregator extends ViewAggregatorIntegrationTestBase {
  private static final int numSourceCluster = 3;
  private static final String stateModel = "LeaderStandby";
  private static final int numResourcePerSourceCluster = 3;
  private static final int numResourcePartition = 3;
  private static final int numReplicaPerResourcePartition = 2;
  private static final String resourceNamePrefix = "testResource";
  private static final String viewClusterName = "ViewCluster-TestHelixViewAggregator";
  private static final StateModelParser stateModelParser = new StateModelParser();
  private int _viewClusterRefreshPeriodSec = 5;
  private ConfigAccessor _configAccessor;
  private HelixAdmin _helixAdmin;
  private MockViewClusterSpectator _monitor;
  private Set<String> _allResources = new HashSet<>();
  private DistViewAggregatorStateModel _viewAggregatorStateModel;

  @BeforeClass
  public void beforeClass() throws Exception {
    // Set up source clusters
    super.beforeClass();

    // Setup tools
    _configAccessor = new ConfigAccessor(_gZkClient);
    _helixAdmin = new ZKHelixAdmin(_gZkClient);

    // Set up view cluster
    _gSetupTool.addCluster(viewClusterName, true);
    ClusterConfig viewClusterConfig = new ClusterConfig(viewClusterName);
    viewClusterConfig.setViewCluster();
    viewClusterConfig.setViewClusterRefreshPeriod(_viewClusterRefreshPeriodSec);
    List<ViewClusterSourceConfig> sourceConfigs = new ArrayList<>();
    for (String sourceClusterName : _allSourceClusters) {
      // We are going to aggregate all supported properties
      sourceConfigs.add(new ViewClusterSourceConfig(sourceClusterName, ZK_ADDR,
          ViewClusterSourceConfig.getValidPropertyTypes()));
    }
    viewClusterConfig.setViewClusterSourceConfigs(sourceConfigs);
    _configAccessor.setClusterConfig(viewClusterName, viewClusterConfig);

    // Set up view cluster monitor
    _monitor = new MockViewClusterSpectator(viewClusterName, ZK_ADDR);

    _viewAggregatorStateModel = new DistViewAggregatorStateModel(ZK_ADDR);
    triggerViewAggregatorStateTransition("OFFLINE", "STANDBY");
  }

  private void triggerViewAggregatorStateTransition(String fromState, String toState)
      throws Exception {
    if (!_viewAggregatorStateModel.getCurrentState().equalsIgnoreCase(fromState)) {
      throw new IllegalStateException(String
          .format("From state (%s) != current state (%s).", fromState,
              _viewAggregatorStateModel.getCurrentState()));
    } else if (_viewAggregatorStateModel.getCurrentState().equalsIgnoreCase(toState)) {
      return;
    }
    NotificationContext context = new NotificationContext(null);
    Message msg = new Message(Message.MessageType.STATE_TRANSITION, "msgId");
    msg.setPartitionName(viewClusterName);
    msg.setFromState(fromState);
    msg.setToState(toState);
    Method method = stateModelParser.getMethodForTransition(_viewAggregatorStateModel.getClass(),
        fromState, toState, new Class[] { Message.class, NotificationContext.class });
    method.invoke(_viewAggregatorStateModel, msg, context);
    _viewAggregatorStateModel.updateState(toState);
  }

  @AfterClass
  public void afterClass() throws Exception {
    _monitor.shutdown();
    super.afterClass();
  }

  @Test
  public void testHelixViewAggregator() throws Exception {
    // Clean up initial events
    _monitor.reset();

    // Start view aggregator
    triggerViewAggregatorStateTransition("STANDBY", "LEADER");

    // Wait for refresh and verify
    Thread.sleep((_viewClusterRefreshPeriodSec + 2) * 1000);
    verifyViewClusterEventChanges(false, true, true);
    Set<String> allParticipantNames = new HashSet<>();
    for (MockParticipantManager participant : _allParticipants) {
      allParticipantNames.add(participant.getInstanceName());
    }
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.LIVEINSTANCES)),
        allParticipantNames);
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.INSTANCES)),
        allParticipantNames);
    _monitor.reset();

    // Create resource and trigger rebalance
    createResources();
    rebalanceResources();

    // Wait for refresh and verify
    Thread.sleep((_viewClusterRefreshPeriodSec + 2) * 1000);
    verifyViewClusterEventChanges(true, false, false);
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.EXTERNALVIEW)),
        _allResources);
    _monitor.reset();

    // Remove 1 resource from a cluster, we should get corresponding changes in view cluster
    List<String> resourceNameList = new ArrayList<>(_allResources);
    _gSetupTool.dropResourceFromCluster(_allSourceClusters.get(0), resourceNameList.get(0));
    rebalanceResources();

    // Wait for refresh and verify
    Thread.sleep((_viewClusterRefreshPeriodSec + 2) * 1000);
    verifyViewClusterEventChanges(true, false, false);
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.EXTERNALVIEW)), _allResources);
    _monitor.reset();

    // Modify view cluster config
    _viewClusterRefreshPeriodSec = 3;
    List<PropertyType> newProperties =
        new ArrayList<>(ViewClusterSourceConfig.getValidPropertyTypes());
    newProperties.remove(PropertyType.LIVEINSTANCES);
    resetViewClusterConfig(_viewClusterRefreshPeriodSec, newProperties);

    // Wait for refresh and verify
    Thread.sleep((_viewClusterRefreshPeriodSec + 2) * 1000);
    verifyViewClusterEventChanges(false, false, true);
    Assert.assertEquals(_monitor.getPropertyNamesFromViewCluster(PropertyType.LIVEINSTANCES).size(),
        0);
    _monitor.reset();

    // Simulate view aggregator service crashed and got reset
    triggerViewAggregatorStateTransition("LEADER", "STANDBY");
    _viewAggregatorStateModel
        .rollbackOnError(new Message(Message.MessageType.STATE_TRANSITION, "test"),
            new NotificationContext(null), null);
    _viewAggregatorStateModel.updateState("ERROR");
    triggerViewAggregatorStateTransition("ERROR", "OFFLINE");

    // Change happened during view aggregator down
    newProperties = new ArrayList<>(ViewClusterSourceConfig.getValidPropertyTypes());
    newProperties.remove(PropertyType.INSTANCES);
    resetViewClusterConfig(_viewClusterRefreshPeriodSec, newProperties);
    MockParticipantManager participant = _allParticipants.get(0);

    participant.syncStop();
    _helixAdmin.enableInstance(participant.getClusterName(), participant.getInstanceName(), false);
    _gSetupTool.dropInstanceFromCluster(participant.getClusterName(), participant.getInstanceName());
    rebalanceResources();
    allParticipantNames.remove(participant.getInstanceName());

    // Restart helix view aggregator
    triggerViewAggregatorStateTransition("OFFLINE", "STANDBY");
    triggerViewAggregatorStateTransition("STANDBY", "LEADER");

    // Wait for refresh and verify
    Thread.sleep((_viewClusterRefreshPeriodSec + 2) * 1000);
    verifyViewClusterEventChanges(true, true, true);
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.EXTERNALVIEW)),
        _allResources);
    Assert.assertEquals(
        new HashSet<>(_monitor.getPropertyNamesFromViewCluster(PropertyType.LIVEINSTANCES)),
        allParticipantNames);
    Assert.assertEquals(_monitor.getPropertyNamesFromViewCluster(PropertyType.INSTANCES).size(), 0);

    // Stop view aggregator
    triggerViewAggregatorStateTransition("LEADER", "STANDBY");
  }

  private void resetViewClusterConfig(int refreshPeriod, List<PropertyType> properties) {
    List<ViewClusterSourceConfig> sourceConfigs = new ArrayList<>();
    for (String sourceCluster : _allSourceClusters) {
      sourceConfigs.add(new ViewClusterSourceConfig(sourceCluster, ZK_ADDR, properties));
    }

    ClusterConfig viewClusterConfig = _configAccessor.getClusterConfig(viewClusterName);
    viewClusterConfig.setViewClusterRefreshPeriod(refreshPeriod);
    viewClusterConfig.setViewClusterSourceConfigs(sourceConfigs);
    _configAccessor.setClusterConfig(viewClusterName, viewClusterConfig);
  }

  private void verifyViewClusterEventChanges(boolean externalViewChange,
      boolean instanceConfigChange, boolean liveInstancesChange) {
    Assert.assertEquals(_monitor.getExternalViewChangeCount() > 0, externalViewChange);
    Assert.assertEquals(_monitor.getInstanceConfigChangeCount() > 0, instanceConfigChange);
    Assert.assertEquals(_monitor.getLiveInstanceChangeCount() > 0, liveInstancesChange);
  }

  /**
   * Create same sets of resources for each cluster
   */
  private void createResources() {
    System.out.println("Creating resources ...");
    for (String sourceClusterName : _allSourceClusters) {
      for (int i = 0; i < numResourcePerSourceCluster; i++) {
        String resourceName = resourceNamePrefix + i;
        _gSetupTool.addResourceToCluster(sourceClusterName, resourceName, numResourcePartition,
            stateModel);
        _allResources.add(resourceName);
      }
    }
  }

  /**
   * Rebalance all resources on each cluster
   */
  private void rebalanceResources() {
    System.out.println("Rebalancing resources ...");
    for (String sourceClusterName : _allSourceClusters) {
      for (String resourceName : _allResources) {
        // We always rebalance all resources, even if it would be deleted during test
        // We assume rebalance will be successful
        try {
          _gSetupTool
              .rebalanceResource(sourceClusterName, resourceName, numReplicaPerResourcePartition);
        } catch (HelixException e) {
          // ok
        }
      }
    }
  }

  @Override
  protected int getNumSourceCluster() {
    return numSourceCluster;
  }
}
