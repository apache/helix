package org.apache.helix.integration.controller;

import java.util.Collections;

import org.apache.helix.HelixConstants;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ParticipantHistory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class TestOfflineNodeTimeoutDuringMaintenanceMode extends ZkTestBase {
  private static final String CLUSTER_NAME = CLUSTER_PREFIX + "_" + TestHelper.getTestClassName();

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    _gSetupTool.addCluster(CLUSTER_NAME, true);
  }

  @Test
  public void testOfflineNodeTimeoutDuringMaintenanceMode() {
    String instanceName = "TestInstance";
    _gSetupTool.addInstanceToCluster(CLUSTER_NAME, instanceName);
    HelixDataAccessor helixDataAccessor = new ZKHelixDataAccessor(CLUSTER_NAME, _baseAccessor);
    PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();

    // Set timeout window to be 0 millisecond, causing any node that goes offline to time out
    ClusterConfig clusterConfig = helixDataAccessor.getProperty(keyBuilder.clusterConfig());
    clusterConfig.setOfflineNodeTimeOutForMaintenanceMode(0L);
    helixDataAccessor.setProperty(keyBuilder.clusterConfig(), clusterConfig);

    // Start and stop an instance, simulating a node offline situation
    MockParticipantManager participant =
        new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    Assert.assertNotNull(helixDataAccessor.getProperty(keyBuilder.liveInstance(instanceName)));
    participant.syncStop();
    Assert.assertNull(helixDataAccessor.getProperty(keyBuilder.liveInstance(instanceName)));

    // Cache refresh after instance goes offline, also enable the maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, true, "Test", Collections.emptyMap());
    ResourceControllerDataProvider resourceControllerDataProvider =
        new ResourceControllerDataProvider(CLUSTER_NAME);
    resourceControllerDataProvider.refresh(helixDataAccessor);

    // Restart the instance
    participant = new MockParticipantManager(ZK_ADDR, CLUSTER_NAME, instanceName);
    participant.syncStart();
    Assert.assertNotNull(helixDataAccessor.getProperty(keyBuilder.liveInstance(instanceName)));

    // Cache refresh, the instance should not be included in liveInstanceCache
    resourceControllerDataProvider.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    resourceControllerDataProvider.refresh(helixDataAccessor);
    Assert.assertFalse(
        resourceControllerDataProvider.getLiveInstances(true).containsKey(instanceName));
    // History wise, it should still be treated as online
    ParticipantHistory history =
        helixDataAccessor.getProperty(keyBuilder.participantHistory(instanceName));
    Assert.assertEquals(history.getLastOfflineTime(), ParticipantHistory.ONLINE);

    // Exit the maintenance mode
    _gSetupTool.getClusterManagementTool()
        .manuallyEnableMaintenanceMode(CLUSTER_NAME, false, "Test", Collections.emptyMap());
    resourceControllerDataProvider.refresh(helixDataAccessor);

    // Cache refresh, the instance should not be included in liveInstanceCache
    resourceControllerDataProvider.notifyDataChange(HelixConstants.ChangeType.LIVE_INSTANCE);
    resourceControllerDataProvider.refresh(helixDataAccessor);
    Assert.assertTrue(
        resourceControllerDataProvider.getLiveInstances(true).containsKey(instanceName));
  }
}
