package org.apache.helix.gateway.service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.statemodel.HelixGatewayOnlineOfflineStateModelFactory;
import org.apache.helix.manager.zk.ParticipantManager;


public class HelixGatewayService {
  final private Map<String, Map<String, HelixManager>> _participantsMap;

  final private String _zkAddress;
  private final GatewayServiceManager _gatewayServiceManager;
  private Map<String, Map<String, AtomicBoolean>> _flagMap;
  public HelixGatewayService(GatewayServiceManager gatewayServiceManager, String zkAddress) {
    _participantsMap = new ConcurrentHashMap<>();
    _zkAddress = zkAddress;
    _gatewayServiceManager = gatewayServiceManager;
    _flagMap = new ConcurrentHashMap<>();
  }

  public GatewayServiceManager getClusterManager() {
    return _gatewayServiceManager;
  }

  public void start() {
    System.out.println("Starting Helix Gateway Service");
  }

  public void registerParticipant() {
    HelixManager manager  = null;
    manager.getStateMachineEngine().registerStateModelFactory("OnlineOffline",
        new HelixGatewayOnlineOfflineStateModelFactory(_gatewayServiceManager));
    try {
      manager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deregisterParticipant(String clusterName, String participantName) {
    HelixManager manager = _participantsMap.get(clusterName).remove(participantName);
    if (manager != null) {
      manager.disconnect();
      removeChannel(participantName);
    }
  }

  public void addChannel() {
   // _flagMap.computeIfAbsent(mockApplication.getInstanceName(), k -> new ConcurrentHashMap<>());
  }

  public void removeChannel(String instanceName) {
    _flagMap.remove(instanceName);
  }

  public AtomicBoolean sendMessage() {
   /* _gatewayServiceManager.sendRequest(request);

      _flagMap.computeIfAbsent(request.getInstanceName(), k -> new ConcurrentHashMap<>())
          .put(request.getMessageId(), flag);
          */
      AtomicBoolean flag = new AtomicBoolean(false);
      return flag;
  }

  public void receiveSTResponse() {
     // AtomicBoolean flag = _flagMap.get(instanceName).remove(response.getMessageId());
  }

  public void newParticipantConnecting(){

  }

  public void participantDisconnected(){

  }

  public void stop() {
    System.out.println("Stopping Helix Gateway Service");
  }
}
