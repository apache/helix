package org.apache.helix.gateway.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.mock.MockApplication;

public class HelixGatewayService {
  final private Map<String, Map<String, HelixManager>> _participantsMap;

  final private String _zkAddress;
  private final ClusterManager _clusterManager;

  public HelixGatewayService(String zkAddress) {
    _participantsMap = new ConcurrentHashMap<>();
    _zkAddress = zkAddress;
    _clusterManager = new ClusterManager();
  }

  public ClusterManager getClusterManager() {
    return _clusterManager;
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
        new HelixGatewayOnlineOfflineStateModelFactory(_clusterManager));
    try {
      _clusterManager.addChannel(mockApplication);
      manager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deregisterParticipant(String clusterName, String participantName) {
    HelixManager manager = _participantsMap.get(clusterName).remove(participantName);
    if (manager != null) {
      manager.disconnect();
      _clusterManager.removeChannel(participantName);
    }
  }

  public void stop() {
    System.out.println("Stoping Helix Gateway Service");
  }
}
