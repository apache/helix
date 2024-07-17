package org.apache.helix.gateway.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.mock.MockApplication;
import org.apache.helix.gateway.statemodel.HelixGatewayOnlineOfflineStateModelFactory;


public class HelixGatewayService {
  final private Map<String, Map<String, HelixManager>> _participantsMap;

  final private String _zkAddress;
  private final GatewayServiceManager _gatewayServiceManager;

  public HelixGatewayService(GatewayServiceManager gatewayServiceManager, String zkAddress) {
    _participantsMap = new ConcurrentHashMap<>();
    _zkAddress = zkAddress;
    _gatewayServiceManager = gatewayServiceManager;
  }

  public GatewayServiceManager getClusterManager() {
    return _gatewayServiceManager;
  }

  public void start() {
    System.out.println("Starting Helix Gateway Service");
  }

  public void registerParticipant(MockApplication mockApplication) {
    HelixManager manager = _participantsMap.computeIfAbsent(mockApplication.getClusterName(),
        k -> new ConcurrentHashMap<>()).computeIfAbsent(mockApplication.getInstanceName(),
        k -> HelixManagerFactory.getZKHelixManager(mockApplication.getClusterName(),
            mockApplication.getInstanceName(), InstanceType.PARTICIPANT, _zkAddress));
    manager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new HelixGatewayOnlineOfflineStateModelFactory(_gatewayServiceManager));
    try {
      _gatewayServiceManager.addChannel(mockApplication);
      manager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deregisterParticipant(String clusterName, String participantName) {
    HelixManager manager = _participantsMap.get(clusterName).remove(participantName);
    if (manager != null) {
      manager.disconnect();
      _gatewayServiceManager.removeChannel(participantName);
    }
  }

  public void stop() {
    System.out.println("Stoping Helix Gateway Service");
  }
}
