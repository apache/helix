package org.apache.helix.integration;

import java.util.Date;
import java.util.List;

import org.apache.helix.InstanceType;
import org.apache.helix.MessageListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.PropertyKey;
import org.apache.helix.TestHelper;
import org.apache.helix.ZNRecord;
import org.apache.helix.common.ZkTestBase;
import org.apache.helix.integration.manager.ClusterControllerManager;
import org.apache.helix.integration.manager.MockParticipantManager;
import org.apache.helix.manager.zk.ZKHelixManager;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.model.Message;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestSyncSessionToController extends ZkTestBase {
  @Test
  public void testSyncSessionToController() throws Exception {
    System.out.println("START testSyncSessionToController at " + new Date(System.currentTimeMillis()));

    String clusterName = getShortClassName();
    MockParticipantManager[] participants = new MockParticipantManager[5];
    int resourceNb = 10;
    TestHelper.setupCluster(clusterName, ZK_ADDR, 12918, // participant port
        "localhost", // participant name prefix
        "TestDB", // resource name prefix
        resourceNb, // resources
        1, // partitions per resource
        5, // number of nodes
        1, // replicas
        "MasterSlave", true); // do rebalance

    ClusterControllerManager controller =
        new ClusterControllerManager(ZK_ADDR, clusterName, "controller_0");
    controller.syncStart();

    // start participants
    for (int i = 0; i < 5; i++) {
      String instanceName = "localhost_" + (12918 + i);

      participants[i] = new MockParticipantManager(ZK_ADDR, clusterName, instanceName);
      participants[i].syncStart();
    }

    ZKHelixManager zkHelixManager = new ZKHelixManager(clusterName, "controllerMessageListener", InstanceType.CONTROLLER, ZK_ADDR);
    zkHelixManager.connect();
    MockMessageListener mockMessageListener = new MockMessageListener();
    zkHelixManager.addControllerMessageListener(mockMessageListener);

    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    ZkBaseDataAccessor<ZNRecord> accessor = new ZkBaseDataAccessor<ZNRecord>(_gZkClient);
    String path = keyBuilder.liveInstance("localhost_12918").getPath();
    Stat stat = new Stat();
    ZNRecord data = accessor.get(path, stat, 2);
    data.getSimpleFields().put("SESSION_ID", "invalid-id");
    accessor.set(path, data, 2);
    Thread.sleep(2000);

    Assert.assertTrue(mockMessageListener.isSessionSyncMessageSent());
  }

  class MockMessageListener implements MessageListener {
    private boolean sessionSyncMessageSent = false;

    @Override
    public void onMessage(String instanceName, List<Message> messages, NotificationContext changeContext) {
      for (Message message: messages) {
        if (message.getMsgId().equals("SESSION-SYNC")) {
          sessionSyncMessageSent = true;
        }
      }
    }

    public boolean isSessionSyncMessageSent() {
      return sessionSyncMessageSent;
    }
  }
}
