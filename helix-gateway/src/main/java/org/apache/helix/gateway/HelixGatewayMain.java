package org.apache.helix.gateway;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.ConfigAccessor;
import org.apache.helix.HelixAdmin;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.mock.ControllerManager;
import org.apache.helix.gateway.mock.MockApplication;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.service.HelixGatewayService;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.OnlineOfflineSMD;
import org.apache.helix.zookeeper.api.client.RealmAwareZkClient;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordSerializer;
import org.apache.helix.zookeeper.impl.client.ZkClient;

public final class HelixGatewayMain {

  private static final String ZK_ADDRESS = "localhost:2181";
  private static final String CLUSTER_NAME = "TEST_CLUSTER";

  private HelixGatewayMain() {
  }

  public static void main(String[] args) throws InterruptedException {
    RealmAwareZkClient zkClient = new ZkClient(ZK_ADDRESS);
    zkClient.setZkSerializer(new ZNRecordSerializer());
    ConfigAccessor configAccessor = new ConfigAccessor(zkClient);
    HelixAdmin admin = new ZKHelixAdmin(zkClient);
    if (admin.getClusters().isEmpty()) {
      admin.addCluster(CLUSTER_NAME);
      admin.addStateModelDef(CLUSTER_NAME, "OnlineOffline", OnlineOfflineSMD.build());
    }

    ClusterConfig clusterConfig = configAccessor.getClusterConfig(CLUSTER_NAME);
    clusterConfig.getRecord().setSimpleField("allowParticipantAutoJoin", "true");
    configAccessor.updateClusterConfig(CLUSTER_NAME, clusterConfig);

    String resourceName = "Test_Resource";

    if (admin.getResourceIdealState(CLUSTER_NAME, resourceName) == null) {
      admin.addResource(CLUSTER_NAME, resourceName, 2, "OnlineOffline",
          IdealState.RebalanceMode.FULL_AUTO.name(),
          "org.apache.helix.controller.rebalancer.strategy.CrushEdRebalanceStrategy");
      admin.rebalance(CLUSTER_NAME, resourceName, 3);
    }

    ControllerManager controllerManager =
        new ControllerManager(ZK_ADDRESS, CLUSTER_NAME, "CONTROLLER", InstanceType.CONTROLLER);
    controllerManager.syncStart();

    HelixGatewayService service = new HelixGatewayService(new GatewayServiceManager(), ZK_ADDRESS);
    service.start();

    List<MockApplication> mockApplications = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      MockApplication mockApplication =
          new MockApplication("INSTANCE_" + i, CLUSTER_NAME, service.getClusterManager());
      service.registerParticipant(mockApplication);
      mockApplications.add(mockApplication);
    }

    Thread.sleep(100000000);

    MockApplication mockApplication = mockApplications.get(3);
    service.deregisterParticipant(mockApplication.getClusterName(),
        mockApplication.getInstanceName());

    controllerManager.syncStop();
  }
}
