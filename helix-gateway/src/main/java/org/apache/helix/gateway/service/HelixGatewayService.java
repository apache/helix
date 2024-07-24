package org.apache.helix.gateway.service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.gateway.statemodel.HelixGatewayOnlineOfflineStateModelFactory;
import org.apache.helix.manager.zk.ZKHelixManager;


/**
 * A service object for each Helix cluster.
 * This service object manages the Helix participants in the cluster.
 */
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

  /**
   * Register a participant to the Helix cluster.
   * It creates a HelixParticipantManager and connects to the Helix controller.
   */
  public void registerParticipant() {
    // TODO: create participant manager and add to _participantsMap
    HelixManager manager = new ZKHelixManager("clusterName", "instanceName", InstanceType.PARTICIPANT, _zkAddress);
    manager.getStateMachineEngine()
        .registerStateModelFactory("OnlineOffline", new HelixGatewayOnlineOfflineStateModelFactory(_gatewayServiceManager));
    try {
      manager.connect();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Deregister a participant from the Helix cluster when app instance is gracefully stopped or connection lost.
   * @param clusterName
   * @param participantName
   */
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
      AtomicBoolean flag = new AtomicBoolean(false);
      return flag;
  }

  /**
   * Entry point for receive the state transition response from the participant.
   * It will update in memory state accordingly.
   */
  public void receiveSTResponse() {
     // AtomicBoolean flag = _flagMap.get(instanceName).remove(response.getMessageId());
  }

  /**
   * Stop the HelixGatewayService.
   * It stops all participants in the cluster.
   */
  public void stop() {
    // TODO: stop all participants
    System.out.println("Stopping Helix Gateway Service");
  }
}
