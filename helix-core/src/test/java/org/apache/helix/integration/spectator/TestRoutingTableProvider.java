package org.apache.helix.integration.spectator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.spectator.RoutingTableSnapshot;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.HelixClusterVerifier;
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

  static final int PARTITION_NUMBER = 20;
  static final int REPLICA_NUMBER = 3;

  private HelixManager _spectator;
  private List<MockParticipantManager> _participants = new ArrayList<MockParticipantManager>();
  private List<String> _instances = new ArrayList<>();
  private ClusterControllerManager _controller;
  private HelixClusterVerifier _clusterVerifier;
  private RoutingTableProvider _routingTableProvider;
  private RoutingTableProvider _routingTableProvider2;
  private boolean _listenerTestResult = true;

  class MockRoutingTableChangeListener implements RoutingTableChangeListener {
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
    _routingTableProvider = new RoutingTableProvider();
    _spectator = HelixManagerFactory
        .getZKHelixManager(CLUSTER_NAME, "spectator", InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();
    _spectator.addExternalViewChangeListener(_routingTableProvider);
    _spectator.addLiveInstanceChangeListener(_routingTableProvider);
    _spectator.addInstanceConfigChangeListener(_routingTableProvider);

    _routingTableProvider2 = new RoutingTableProvider(_spectator);

    _clusterVerifier = new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verify());
  }

  @AfterClass
  public void afterClass() {
    // stop participants
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }
    _controller.syncStop();
    _routingTableProvider.shutdown();
    _routingTableProvider2.shutdown();
    _spectator.disconnect();
    _gSetupTool.deleteCluster(CLUSTER_NAME);
  }

  @Test
  public void testRoutingTable() {
    Assert.assertEquals(_routingTableProvider.getLiveInstances().size(), _instances.size());
    Assert.assertEquals(_routingTableProvider.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider2.getLiveInstances().size(), _instances.size());
    Assert.assertEquals(_routingTableProvider2.getInstanceConfigs().size(), _instances.size());

    validateRoutingTable(_routingTableProvider, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(1), _instances.get(2)));
    validateRoutingTable(_routingTableProvider2, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(1), _instances.get(2)));

    Collection<String> databases = _routingTableProvider.getResources();
    Assert.assertEquals(databases.size(), 1);
  }

  @Test(dependsOnMethods = { "testRoutingTable" })
  public void testDisableInstance() throws InterruptedException {
    // disable the master instance
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, false);
    Assert.assertTrue(_clusterVerifier.verify());

    validateRoutingTable(_routingTableProvider, Sets.newSet(_instances.get(1)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider2, Sets.newSet(_instances.get(1)),
        Sets.newSet(_instances.get(2)));
  }


  @Test(dependsOnMethods = { "testDisableInstance" })
  public void testRoutingTableListener() throws InterruptedException {
    RoutingTableChangeListener routingTableChangeListener = new MockRoutingTableChangeListener();
    Map<String, Set<String>> context = new HashMap<>();
    context.put("MASTER", Sets.newSet(_instances.get(0)));
    context.put("SLAVE", Sets.newSet(_instances.get(1), _instances.get(2)));
    _routingTableProvider.addRoutingTableChangeListener(routingTableChangeListener, context);
    _routingTableProvider.addRoutingTableChangeListener(new MockRoutingTableChangeListener(), null);
    // reenable the master instance to cause change
    String prevMasterInstance = _instances.get(0);
    _gSetupTool.getClusterManagementTool().enableInstance(CLUSTER_NAME, prevMasterInstance, true);
    Assert.assertTrue(_clusterVerifier.verify());
    Assert.assertTrue(_listenerTestResult);
  }


  @Test(dependsOnMethods = { "testRoutingTableListener" })
  public void testShutdownInstance() throws InterruptedException {
    // shutdown second instance.
    _participants.get(1).syncStop();

    Assert.assertTrue(_clusterVerifier.verify());

    Assert.assertEquals(_routingTableProvider.getLiveInstances().size(), _instances.size() - 1);
    Assert.assertEquals(_routingTableProvider.getInstanceConfigs().size(), _instances.size());

    Assert.assertEquals(_routingTableProvider2.getLiveInstances().size(), _instances.size() - 1);
    Assert.assertEquals(_routingTableProvider2.getInstanceConfigs().size(), _instances.size());

    validateRoutingTable(_routingTableProvider, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(2)));
    validateRoutingTable(_routingTableProvider2, Sets.newSet(_instances.get(0)),
        Sets.newSet(_instances.get(2)));
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

