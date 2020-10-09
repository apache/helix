package org.apache.helix.integration.controller;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.ParticipantHistory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestOfflineNodeTimeoutDuringMaintenanceMode extends ZkTestBase {
  private static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + TestHelper.getTestClassName();

  private HelixDataAccessor _helixDataAccessor;
  private PropertyKey.Builder _keyBuilder;
  private ClusterControllerManager _controller;

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
    String controllerName = CONTROLLER_PREFIX + "_0";
    _controller = new ClusterControllerManager(ZK_ADDR, CLUSTER_NAME, controllerName);
    _controller.syncStart();

    _helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    _keyBuilder = _helixDataAccessor.keyBuilder();
  }

  @Test
  public void testOfflineNodeTimeoutDuringMaintenanceMode() throws Exception {
    // 1st case: an offline node that comes live during maintenance, should be timed-out
    String instance1 = "Instance1";
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance1);
    MockParticipantManager participant1 =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance1);
    participant1.syncStart();
    // 2nd case: an offline node that comes live before maintenance, shouldn't be timed-out
    String instance2 = "Instance2";
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance2);
    MockParticipantManager participant2 =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    participant2.syncStart();
    // New instance case: a new node that comes live after maintenance, shouldn't be timed-out
    String newInstance = "NewInstance";
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, newInstance);

    String dbName = "TestDB_1";
    _gSetupTool.addResourceToCluster(CLUSTER_NAME, dbName, 5, "MasterSlave");
    _gSetupTool.rebalanceStorageCluster(CLUSTER_NAME, dbName, 3);

    // Set timeout window to be 0 millisecond, causing any node that goes offline to time out
    ClusterConfig clusterConfig = _helixDataAccessor.getProperty(_keyBuilder.clusterConfig());
    clusterConfig.setOfflineNodeTimeOutForMaintenanceMode(0L);
    _helixDataAccessor.setProperty(_keyBuilder.clusterConfig(), clusterConfig);

    Assert.assertNotNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance1)));
    Assert.assertNotNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance2)));

    // Start and stop the instance, simulating a node offline situation
    participant1.syncStop();
    Assert.assertNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance1)));
    participant2.syncStop();
    Assert.assertNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance2)));

    // Enable maintenance mode; restart instance2 before and instance 1 after
    participant2 = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance2);
    participant2.syncStart();
    Assert.assertNotNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance2)));
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "Test", Collections.emptyMap());
    participant1 = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance1);
    participant1.syncStart();
    Assert.assertNotNull(_helixDataAccessor.getProperty(_keyBuilder.liveInstance(instance1)));
    MockParticipantManager newParticipant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, newInstance);
    newParticipant.syncStart();

    // Only includes instance 2
    ExternalView externalView = _helixDataAccessor.getProperty(_keyBuilder.externalView(dbName));
    for (String partition : externalView.getPartitionSet()) {
      Assert.assertEquals(externalView.getStateMap(partition).keySet(), Collections.singletonList(instance2));
    }
    // History wise, all instances should still be treated as online
    ParticipantHistory history1 =
        _helixDataAccessor.getProperty(_keyBuilder.participantHistory(instance1));
    Assert.assertEquals(history1.getLastOfflineTime(), ParticipantHistory.ONLINE);
    ParticipantHistory history2 =
        _helixDataAccessor.getProperty(_keyBuilder.participantHistory(instance2));
    Assert.assertEquals(history2.getLastOfflineTime(), ParticipantHistory.ONLINE);

    // Exit the maintenance mode, and the instance should be included in liveInstanceCache again
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "Test", Collections.emptyMap());
    // Include every instance
    final ExternalView newExternalView = _helixDataAccessor.getProperty(_keyBuilder.externalView(dbName));
    Set<String> allInstances = new HashSet<>();
    allInstances.add(instance1);
    allInstances.add(instance2);
    allInstances.add(newInstance);
    for (String partition : externalView.getPartitionSet()) {
      TestHelper.verify(() -> newExternalView.getStateMap(partition).keySet().equals(allInstances),
          12000);
    }
  }

  @Test(dependsOnMethods = "testOfflineNodeTimeoutDuringMaintenanceMode")
  // Testing with mocked timestamps, instead of full integration
  public void testOfflineNodeTimeoutDuringMaintenanceModeTimestampsMock()
      throws InterruptedException {
    // Timeout window is set as 10 milliseconds. Let maintenance mode start time be denoted as T
    List<String> instanceList = new ArrayList<>();
    // 3rd case:
    //  Online: [T-10, T+5, T+20]
    //  Offline: [T+1, T+9]
    // Expected: Timed-out
    String instance3 = "Instance3";
    instanceList.add(instance3);
    // 4th case:
    //  Online: [T-10, T+12]
    //  Offline: [T+1, T+6]
    // Expected: Timed-out
    String instance4 = "Instance4";
    instanceList.add(instance4);
    // 5th case:
    //  Online: [T-10, T+6, T+12]
    //  Offline: [T+1]
    // Expected: Not timed-out
    String instance5 = "Instance5";
    instanceList.add(instance5);
    // 6th case: (X denoting malformed timestamps)
    //  Online: [T-10, X, X, T+12]
    //  Offline: [T+1, X, X]
    // Expected: Timed-out
    String instance6 = "Instance6";
    instanceList.add(instance6);
    // 7th case: (X denoting malformed timestamps)
    //  Online: [T-10, T+1, X, X]
    //  Offline: [X, X, T+10]
    // Expected: Timed-out (by waiting until current time to be at least T+20)
    String instance7 = "Instance7";
    instanceList.add(instance7);
    // 8th case:
    //  Online: [T-10, T+3, T+4, T+8, T+9, T+10]
    //  Offline: [T+1, T+2, T+5, T+6, T+7]
    // Expected: Not timed-out
    String instance8 = "Instance8";
    instanceList.add(instance8);

    ClusterConfig clusterConfig = _helixDataAccessor.getProperty(_keyBuilder.clusterConfig());
    clusterConfig.setOfflineNodeTimeOutForMaintenanceMode(10L);
    _helixDataAccessor.setProperty(_keyBuilder.clusterConfig(), clusterConfig);

    for (String instance : instanceList) {
      _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instance);
      MockParticipantManager participant =
          new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instance);
      participant.syncStart();
    }

    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "Test", Collections.emptyMap());
    long currentTime = System.currentTimeMillis();

    addTimestampsToParticipantHistory(instance3, new long[]{currentTime + 5, currentTime + 20},
        new long[]{currentTime + 1, currentTime + 9});
    addTimestampsToParticipantHistory(instance4, new long[]{currentTime + 12},
        new long[]{currentTime + 1, currentTime + 6});
    addTimestampsToParticipantHistory(instance5, new long[]{currentTime + 6, currentTime + 12},
        new long[]{currentTime + 1});
    addTimestampsToParticipantHistory(instance6, new long[]{-1, -1, currentTime + 12},
        new long[]{currentTime + 1, -1, -1});
    addTimestampsToParticipantHistory(instance7, new long[]{currentTime + 1, -1, -1},
        new long[]{-1, -1, currentTime + 10});
    addTimestampsToParticipantHistory(instance8, new long[]{
            currentTime + 3, currentTime + 4, currentTime + 8, currentTime + 9, currentTime + 10},
        new long[]{
            currentTime + 1, currentTime + 2, currentTime + 5, currentTime + 6, currentTime + 7});

    // Sleep to make instance7 timed out. This ensures the refresh time to be at least
    // currentTime + 10 (last offline timestamp) + 10 (timeout window).
    long timeToSleep = currentTime + 20 - System.currentTimeMillis();
    if (timeToSleep > 0) {
      Thread.sleep(timeToSleep);
    }

    ResourceControllerDataProvider resourceControllerDataProvider =
        new ResourceControllerDataProvider(CLUSTER_NAME);
    resourceControllerDataProvider.refresh(_helixDataAccessor);
    Assert
        .assertFalse(resourceControllerDataProvider.getLiveInstances().containsKey(instance3));
    Assert
        .assertFalse(resourceControllerDataProvider.getLiveInstances().containsKey(instance4));
    Assert.assertTrue(resourceControllerDataProvider.getLiveInstances().containsKey(instance5));
    Assert
        .assertFalse(resourceControllerDataProvider.getLiveInstances().containsKey(instance6));
    Assert
        .assertFalse(resourceControllerDataProvider.getLiveInstances().containsKey(instance7));
    Assert.assertTrue(resourceControllerDataProvider.getLiveInstances().containsKey(instance8));
  }

  /**
   * Add timestamps to an instance's ParticipantHistory; pass -1 to insert malformed strings instead
   */
  private void addTimestampsToParticipantHistory(String instanceName, long[] onlineTimestamps,
      long[] offlineTimestamps) {
    ParticipantHistory history =
        _helixDataAccessor.getProperty(_keyBuilder.participantHistory(instanceName));
    List<String> historyList = history.getRecord().getListField("HISTORY");
    Map<String, String> historySample =
        ParticipantHistory.sessionHistoryStringToMap(historyList.get(0));
    for (long onlineTimestamp : onlineTimestamps) {
      if (onlineTimestamp >= 0) {
        historySample.put("TIME", Long.toString(onlineTimestamp));
      } else {
        historySample.put("TIME", "MalformedString");
      }
      historyList.add(historySample.toString());
    }
    List<String> offlineList = history.getRecord().getListField("OFFLINE");
    if (offlineList == null) {
      offlineList = new ArrayList<>();
      history.getRecord().setListField("OFFLINE", offlineList);
    }
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss:SSS");
    df.setTimeZone(TimeZone.getTimeZone("UTC"));
    for (long offlineTimestamp : offlineTimestamps) {
      if (offlineTimestamp >= 0) {
        offlineList.add(df.format(new Date(offlineTimestamp)));
      } else {
        offlineList.add("MalformedString");
      }
    }
    _helixDataAccessor.setProperty(_keyBuilder.participantHistory(instanceName), history);
  }
}
