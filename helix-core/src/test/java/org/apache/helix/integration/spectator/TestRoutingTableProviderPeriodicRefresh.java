package org.apache.helix.integration.spectator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyType;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.model.BuiltInStateModelDefinitions;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.spectator.RoutingTableProvider;
import org.apache.helix.tools.ClusterVerifiers.BestPossibleExternalViewVerifier;
import org.apache.helix.tools.ClusterVerifiers.ZkHelixClusterVerifier;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestRoutingTableProviderPeriodicRefresh extends ZkTestBase {
  private static final org.slf4j.Logger logger =
      LoggerFactory.getLogger(TestRoutingTableProviderPeriodicRefresh.class);

  private static final String STATE_MODEL = BuiltInStateModelDefinitions.MasterSlave.name();
  private static final String TEST_DB = "TestDB";
  private static final String CLASS_NAME = TestRoutingTableProvider.class.getSimpleName();
  private static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + CLASS_NAME;
  private static final int PARTICIPANT_NUMBER = 3;
  private static final int PARTICIPANT_START_PORT = 12918;

  private static final int PARTITION_NUMBER = 20;
  private static final int REPLICA_NUMBER = 3;

  private HelixManager _spectator;
  private HelixManager _spectator_2;
  private HelixManager _spectator_3;
  private List<MockParticipantManager> _participants = new ArrayList<>();
  private List<String> _instances = new ArrayList<>();
  private ClusterControllerManager _controller;
  private ZkHelixClusterVerifier _clusterVerifier;
  private MockRoutingTableProvider _routingTableProvider;
  private MockRoutingTableProvider _routingTableProviderNoPeriodicRefresh;
  private MockRoutingTableProvider _routingTableProviderLongPeriodicRefresh;

  @BeforeClass
  public void beforeClass() throws Exception {
    System.out
        .println("START " + getShortClassName() + " at " + new Date(System.currentTimeMillis()));

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

    createDBInSemiAuto(_gSetupTool, CLUSTER_NAME, TEST_DB, _instances, STATE_MODEL,
        PARTITION_NUMBER, REPLICA_NUMBER);

    // start controller
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    // start speculator - initialize it with a Mock
    _spectator = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator",
        InstanceType.SPECTATOR, ZK_ADDR);
    _spectator.connect();

    _spectator_2 = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator_2",
        InstanceType.SPECTATOR, ZK_ADDR);
    _spectator_2.connect();

    _spectator_3 = HelixManagerFactory.getZKHelixManager(CLUSTER_NAME, "spectator_3",
        InstanceType.SPECTATOR, ZK_ADDR);
    _spectator_3.connect();

    _routingTableProvider =
        new MockRoutingTableProvider(_spectator, PropertyType.EXTERNALVIEW, true, 1000L);
    _spectator.addExternalViewChangeListener(_routingTableProvider);
    _spectator.addLiveInstanceChangeListener(_routingTableProvider);
    _spectator.addInstanceConfigChangeListener(_routingTableProvider);

    _routingTableProviderNoPeriodicRefresh =
        new MockRoutingTableProvider(_spectator_2, PropertyType.EXTERNALVIEW, false, 1000L);
    _spectator_2.addExternalViewChangeListener(_routingTableProviderNoPeriodicRefresh);
    _spectator_2.addLiveInstanceChangeListener(_routingTableProviderNoPeriodicRefresh);
    _spectator_2.addInstanceConfigChangeListener(_routingTableProviderNoPeriodicRefresh);

    _routingTableProviderLongPeriodicRefresh =
        new MockRoutingTableProvider(_spectator_3, PropertyType.EXTERNALVIEW, true, 3000000L);
    _spectator_3.addExternalViewChangeListener(_routingTableProviderLongPeriodicRefresh);
    _spectator_3.addLiveInstanceChangeListener(_routingTableProviderLongPeriodicRefresh);
    _spectator_3.addInstanceConfigChangeListener(_routingTableProviderLongPeriodicRefresh);

    _clusterVerifier =
        new BestPossibleExternalViewVerifier.Builder(CLUSTER_NAME).setZkClient(_gZkClient).build();
    Assert.assertTrue(_clusterVerifier.verifyByPolling());

  }

  @AfterClass
  public void afterClass() {
    // stop participants
    for (MockParticipantManager p : _participants) {
      p.syncStop();
    }

    _controller.syncStop();
    _spectator.disconnect();
    _spectator_2.disconnect();
    _spectator_3.disconnect();
    deleteCluster(CLUSTER_NAME);
  }

  public class MockRoutingTableProvider extends RoutingTableProvider {
    private volatile int _refreshCount = 0;
    private static final boolean DEBUG = false;

    public MockRoutingTableProvider(HelixManager helixManager, PropertyType sourceDataType,
        boolean isPeriodicRefreshEnabled, long periodRefreshInterval) {
      super(helixManager, sourceDataType, isPeriodicRefreshEnabled, periodRefreshInterval);
    }

    @Override
    protected synchronized void refresh(List<ExternalView> externalViewList,
        NotificationContext changeContext) {
      super.refresh(externalViewList, changeContext);
      _refreshCount++;
      if (DEBUG) {
        print();
      }
    }

    @Override
    protected synchronized void refresh(Collection<ExternalView> externalViews,
        Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
      super.refresh(externalViews, instanceConfigs, liveInstances);
      _refreshCount++;
      if (DEBUG) {
        print();
      }
    }

    @Override
    protected synchronized void refresh(
        Map<String, Map<String, Map<String, CurrentState>>> currentStateMap,
        Collection<InstanceConfig> instanceConfigs, Collection<LiveInstance> liveInstances) {
      super.refresh(currentStateMap, instanceConfigs, liveInstances);
      _refreshCount++;
      if (DEBUG) {
        print();
      }
    }

    // Log statements for debugging purposes
    private void print() {
      logger.error("Refresh happened; count: {}", getRefreshCount());
      logger.error("timestamp: {}", System.currentTimeMillis());
    }

    synchronized int getRefreshCount() {
      return _refreshCount;
    }
  }

  @Test
  public void testPeriodicRefresh() throws InterruptedException {
    // Wait so that initial refreshes finish (not triggered by periodic refresh timer)
    Thread.sleep(1000L);

    // Test short refresh
    int prevRefreshCount = _routingTableProvider.getRefreshCount();
    // Wait for one timer duration
    Thread.sleep(1000L);
    // The timer should have gone off, incrementing the refresh count
    Assert.assertEquals(_routingTableProvider.getRefreshCount(), prevRefreshCount + 1);

    // Test no periodic refresh
    prevRefreshCount = _routingTableProviderNoPeriodicRefresh.getRefreshCount();
    // Wait
    Thread.sleep(2000);
    // The timer should NOT have gone off, the refresh count must stay the same
    Assert.assertEquals(_routingTableProviderNoPeriodicRefresh.getRefreshCount(), prevRefreshCount);

    // Test long periodic refresh
    prevRefreshCount = _routingTableProviderLongPeriodicRefresh.getRefreshCount();
    // Wait
    Thread.sleep(2000);
    // The timer should NOT have gone off yet, the refresh count must stay the same
    Assert.assertEquals(_routingTableProviderLongPeriodicRefresh.getRefreshCount(),
        prevRefreshCount);

    // Call shutdown to make sure they are shutting down properly
    _routingTableProvider.shutdown();
    _routingTableProviderNoPeriodicRefresh.shutdown();
    _routingTableProviderLongPeriodicRefresh.shutdown();
  }
}
