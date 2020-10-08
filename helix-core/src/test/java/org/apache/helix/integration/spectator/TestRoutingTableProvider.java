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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyPathBuilder;
import org.apache.helix.PropertyType;
import org.apache.helix.TestHelper;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.mockito.internal.util.collections.Sets;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProvider extends ZkTestBase {

  static final String STATE_MODEL = BuiltInStateModelDefinitions.MasterSlave.name();
  static final String TEST_DB = "TestDB";
  static final String CLASS_NAME = TestRoutingTableProvider.class.getSimpleName();
  static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  static final int PARTICIPANT_NUMBER = 3;
  static final int PARTICIPANT_START_PORT = 12918;
  static final long WAIT_DURATION = 5 * 1000L; // 5 seconds

  static final int PARTITION_NUMBER = 20;
  static final int REPLICA_NUMBER = 3;

  private HelixManager _spectator;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private List<String> _instances = new ArrayList<>();
  private ClusterControllerManager _controller;
  private ZkHelixClusterVerifier _clusterVerifier;
  private RoutingTableProvider _routingTableProvider_default;
  private RoutingTableProvider _routingTableProvider_ev;
  private RoutingTableProvider _routingTableProvider_cs;
  private boolean _listenerTestResult = true;


  private static final AtomicBoolean customizedViewChangeCalled = new AtomicBoolean(false);

  class MockRoutingTableChangeListener implements RoutingTableChangeListener {
    boolean routingTableChangeReceived = false;

    @Override
    public void onRoutingTableChange(RoutingTableSnapshot routingTableSnapshot, Object context) {
      Set<String> masterInstances = new HashSet<>();
      Set<String> slaveInstances = new HashSet<>();
      for (InstanceConfig config : routingTableSnapshot
          .getInstancesForResource(TEST_DB, "MASTER")) {
        masterInstances.add(config.getInstanceName());
      }
      for (InstanceConfig config : routingTableSnapshot.getInstancesForResource(TEST_DB, "SLAVE")) {
        slaveInstances.add(config.getInstanceName());
      }
      if (context != null && (!masterInstances.equals(Map.class.cast(context).get("MASTER"))
          || !slaveInstances.equals(Map.class.cast(context).get("SLAVE")))) {
        _listenerTestResult = false;
      } else {
        _listenerTestResult = true;
      }
      routingTableChangeReceived = true;
    }
  }

  class MockRoutingTableProvider extends RoutingTableProvider {
    MockRoutingTableProvider(HelixManager helixManager, Map<PropertyType, List<String>> sourceDataTypes) {
      super(helixManager, sourceDataTypes);
    }

    @Override
    public void onCustomizedViewChange(List<CustomizedView> customizedViewList,
        NotificationContext changeContext){
      customizedViewChangeCalled.getAndSet(true);
      super.onCustomizedViewChange(customizedViewList, changeContext);
    }
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out.println(
        "START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

    // setup storage cluster
    _gSetupTool.addCluster(CLUSTER_NAME, true);

    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      String instance = PARTICIPANT_PREFIX + "_" + (PARTICIPANT_START_PORT + i);
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      _instances.add(instance);
    }

    // start dummy participants
    for (int i = 0; i < PARTICIPANT_NUMBER; i++) {
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, _instances.get(i));
      participant.syncStart();
      _participants.add(participant);
    }

    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, TEST_DB, _instances,
        STATE_MODEL, PARTITION_NUMBER, REPLICA_NUMBER);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // start speculator
    _spectator = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();
    _routingTableProvider_default = new RoutingTableProvider(_spectator);
    _spectator.addExternalViewChangeListener(_routingTableProvider_default);
    _spectator.addLiveInstanceChangeListener(_routingTableProvider_default);
    _spectator.addInstanceConfigChangeListener(_routingTableProvider_default);

    _routingTableProvider_ev = new RoutingTableProvider(_spectator);
    _routingTableProvider_cs = new RoutingTableProvider(_spectator, PropertyType.CURRENTSTATES);

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient)
        .setWaitTillVerify(TestHelper.DEFAULT_REBALANCE_PROCESSING_WAIT_TIME)
        .build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
  }

  @AfterClass
  public void afterClass() {
    // stop participants
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _controller.syncStop();
    _routingTableProvider_default.shutdown();
    _routingTableProvider_ev.shutdown();
    _routingTableProvider_cs.shutdown();
    _spectator.disconnect();
    deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testInvocation() throws Exception {
    MockRoutingTableChangeListener routingTableChangeListener = new MockRoutingTableChangeListener();
    _routingTableProvider_default
        .addRoutingTableChangeListener(routingTableChangeListener, null, true);

    // Add a routing table provider listener should trigger an execution of the
    // listener callbacks
    Assert.assertTrue(TestHelper.verify(() -> {
      if (!routingTableChangeListener.routingTableChangeReceived) {
        return false;
      }
      return true;
    }, TestHelper.WAIT_DURATION));
  }

  @Test(dependsOnMethods = { "testInvocation" })
  public void testRoutingTable() {
    Assert.assertEquals(_routingTableProvider_default.getLiveInstances().size(), _instances.size());
    Assert.assertEquals(_routingTableProvider_default.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider_ev.getLiveInstances().size(), _instances.size());
    Assert.assertEquals(_routingTableProvider_ev.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider_cs.getLiveInstances().size(), _instances.size());
    Assert.assertEquals(_routingTableProvider_cs.getInstanceConfigs().size(), _instances.size());

    validateRoutingTable(_routingTableProvider_default, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(1), _instances.get(2)));
    validateRoutingTable(_routingTableProvider_ev, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(1), _instances.get(2)));
    validateRoutingTable(_routingTableProvider_cs, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(1), _instances.get(2)));

    Collection<String> databases = _routingTableProvider_default.getResources();
    Assert.assertEquals(databases.size(), 1);
  }

  @Test(dependsOnMethods = { "testRoutingTable" })
  public void testDisableInstance() throws InterruptedException {
    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    validateRoutingTable(_routingTableProvider_default, Sets.newSet(_instances.get(1)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider_ev, Sets.newSet(_instances.get(1)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider_cs, Sets.newSet(_instances.get(1)),
        Sets.newSet(_instances.get(2)));
  }


  @Test(dependsOnMethods = { "testDisableInstance" })
  public void testRoutingTableListener() throws InterruptedException {
    RoutingTableChangeListener routingTableChangeListener = new MockRoutingTableChangeListener();
    Map<String, Set<String>> context = new HashMap<>();
    context.put("MASTER", Sets.newSet(_instances.get(0)));
    context.put("SLAVE", Sets.newSet(_instances.get(1), _instances.get(2)));
    _routingTableProvider_default
        .addRoutingTableChangeListener(routingTableChangeListener, context, true);
    _routingTableProvider_default
        .addRoutingTableChangeListener(new MockRoutingTableChangeListener(), null, true);
    // reenable the master instance to cause change
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verifyByPolling());
    Assert.assertTrue(_listenerTestResult);
  }


  @Test(dependsOnMethods = { "testRoutingTableListener" })
  public void testShutdownInstance() throws InterruptedException {
    // shutdown second instance.
    _participants.get(1).syncStop();

    Assert.assertTrue(_clusterVerifier.verifyByPolling());

    Assert.assertEquals(_routingTableProvider_default.getLiveInstances().size(), _instances.size() - 1);
    Assert.assertEquals(_routingTableProvider_default.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider_ev.getLiveInstances().size(), _instances.size() - 1);
    Assert.assertEquals(_routingTableProvider_ev.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider_cs.getLiveInstances().size(), _instances.size() - 1);
    Assert.assertEquals(_routingTableProvider_cs.getInstanceConfigs().size(), _instances.size());

    validateRoutingTable(_routingTableProvider_default, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider_ev, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider_cs, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(2)));
  }

  @Test(expectedExceptions = HelixException.class, dependsOnMethods = "testShutdownInstance")
  public void testExternalViewWithType() {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.EXTERNALVIEW, Arrays.asList("typeA"));
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA", "typeB"));
    RoutingTableProvider routingTableProvider;
    routingTableProvider = new RoutingTableProvider(_spectator, sourceDataTypes);
  }

  @Test(dependsOnMethods = "testExternalViewWithType", expectedExceptions = HelixException.class)
  public void testCustomizedViewWithoutType() {
    RoutingTableProvider routingTableProvider;
    routingTableProvider = new RoutingTableProvider(_spectator, PropertyType.CUSTOMIZEDVIEW);
  }

  @Test(dependsOnMethods = "testCustomizedViewWithoutType")
  public void testCustomizedViewCorrectConstructor() throws Exception {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA"));
    MockRoutingTableProvider routingTableProvider =
        new MockRoutingTableProvider(_spectator, sourceDataTypes);

    CustomizedView customizedView = new CustomizedView(TEST_DB);
    customizedView.setState("p1", "h1", "testState");

    // Clear the flag before writing to the Customized View Path
    customizedViewChangeCalled.getAndSet(false);
    String customizedViewPath = PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeA", TEST_DB);
    _spectator.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath,
        customizedView.getRecord(), AccessOption.PERSISTENT);

    boolean onCustomizedViewChangeCalled =
        TestHelper.verify(() -> customizedViewChangeCalled.get(), WAIT_DURATION);
    Assert.assertTrue(onCustomizedViewChangeCalled);

    _spectator.getHelixDataAccessor().getBaseDataAccessor().remove(customizedViewPath,
        AccessOption.PERSISTENT);
    routingTableProvider.shutdown();
  }

  @Test(dependsOnMethods = "testCustomizedViewCorrectConstructor")
  public void testGetRoutingTableSnapshot() throws Exception {
    Map<PropertyType, List<String>> sourceDataTypes = new HashMap<>();
    sourceDataTypes.put(PropertyType.CUSTOMIZEDVIEW, Arrays.asList("typeA", "typeB"));
    sourceDataTypes.put(PropertyType.EXTERNALVIEW, Collections.emptyList());
    RoutingTableProvider routingTableProvider =
        new RoutingTableProvider(_spectator, sourceDataTypes);

    CustomizedView customizedView1 = new CustomizedView("Resource1");
    customizedView1.setState("p1", "h1", "testState1");
    customizedView1.setState("p1", "h2", "testState1");
    customizedView1.setState("p2", "h1", "testState2");
    customizedView1.setState("p3", "h2", "testState3");
    String customizedViewPath1 =
        PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeA", "Resource1");

    CustomizedView customizedView2 = new CustomizedView("Resource2");
    customizedView2.setState("p1", "h3", "testState3");
    customizedView2.setState("p1", "h4", "testState2");
    String customizedViewPath2 =
        PropertyPathBuilder.customizedView(CLUSTER_NAME, "typeB", "Resource2");

    _spectator.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath1,
        customizedView1.getRecord(), AccessOption.PERSISTENT);
    _spectator.getHelixDataAccessor().getBaseDataAccessor().set(customizedViewPath2,
        customizedView2.getRecord(), AccessOption.PERSISTENT);

    RoutingTableSnapshot routingTableSnapshot =
        routingTableProvider.getRoutingTableSnapshot(PropertyType.CUSTOMIZEDVIEW, "typeA");
    Assert.assertEquals(routingTableSnapshot.getPropertyType(), PropertyType.CUSTOMIZEDVIEW);
    Assert.assertEquals(routingTableSnapshot.getCustomizedStateType(), "typeA");

    routingTableSnapshot =
        routingTableProvider.getRoutingTableSnapshot(PropertyType.CUSTOMIZEDVIEW, "typeB");
    Assert.assertEquals(routingTableSnapshot.getPropertyType(), PropertyType.CUSTOMIZEDVIEW);
    Assert.assertEquals(routingTableSnapshot.getCustomizedStateType(), "typeB");

    // Make sure snapshot information is correct
    // Check resources are in a correct state
    boolean isRoutingTableUpdatedProperly = TestHelper.verify(() -> {
      Map<String, Map<String, RoutingTableSnapshot>> routingTableSnapshots =
          routingTableProvider.getRoutingTableSnapshots();
      RoutingTableSnapshot routingTableSnapshotTypeA =
          routingTableSnapshots.get(PropertyType.CUSTOMIZEDVIEW.name()).get("typeA");
      RoutingTableSnapshot routingTableSnapshotTypeB =
          routingTableSnapshots.get(PropertyType.CUSTOMIZEDVIEW.name()).get("typeB");
      String typeAp1h1 = "noState";
      String typeAp1h2 = "noState";
      String typeAp2h1 = "noState";
      String typeAp3h2 = "noState";
      String typeBp1h2 = "noState";
      String typeBp1h4 = "noState";
      try {
        typeAp1h1 = routingTableSnapshotTypeA.getCustomizeViews().iterator().next()
            .getStateMap("p1").get("h1");
        typeAp1h2 = routingTableSnapshotTypeA.getCustomizeViews().iterator().next()
            .getStateMap("p1").get("h2");
        typeAp2h1 = routingTableSnapshotTypeA.getCustomizeViews().iterator().next()
            .getStateMap("p2").get("h1");
        typeAp3h2 = routingTableSnapshotTypeA.getCustomizeViews().iterator().next()
            .getStateMap("p3").get("h2");
        typeBp1h2 = routingTableSnapshotTypeB.getCustomizeViews().iterator().next()
            .getStateMap("p1").get("h3");
        typeBp1h4 = routingTableSnapshotTypeB.getCustomizeViews().iterator().next()
            .getStateMap("p1").get("h4");
      } catch (Exception e) {
        // ok because RoutingTable has not been updated yet
        return false;
      }
      return (routingTableSnapshots.size() == 2
          && routingTableSnapshots.get(PropertyType.CUSTOMIZEDVIEW.name()).size() == 2
          && typeAp1h1.equals("testState1") && typeAp1h2.equals("testState1")
          && typeAp2h1.equals("testState2") && typeAp3h2.equals("testState3")
          && typeBp1h2.equals("testState3") && typeBp1h4.equals("testState2"));
    }, WAIT_DURATION);
    Assert.assertTrue(isRoutingTableUpdatedProperly, "RoutingTable has been updated properly");
    routingTableProvider.shutdown();
  }

  private void validateRoutingTable(RoutingTableProvider routingTableProvider,
      Set<String> masterNodes, Set<String> slaveNodes) {
    IdealState is =
        _gSetupTool.getClusterManagementTool().getResourceIdealState(CLUSTER_NAME, TEST_DB);
    for (String p : is.getPartitionSet()) {
      Set<String> masterInstances = new HashSet<>();
      for (InstanceConfig config : routingTableProvider.getInstances(TEST_DB, p, "MASTER")) {
        masterInstances.add(config.getInstanceName());
      }

      Set<String> slaveInstances = new HashSet<>();
      for (InstanceConfig config : routingTableProvider.getInstances(TEST_DB, p, "SLAVE")) {
        slaveInstances.add(config.getInstanceName());
      }

      Assert.assertEquals(masterInstances, masterNodes);
      Assert.assertEquals(slaveInstances, slaveNodes);
    }
  }
}

