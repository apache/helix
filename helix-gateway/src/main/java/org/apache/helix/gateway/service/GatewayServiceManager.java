package org.apache.helix.gateway.service;

import java.util.Map;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.gateway.grpcservice.HelixGatewayServiceService;


/**
 * A top layer class that send/receive messages from Grpc end point, and dispatch them to corrsponding gateway services.
 *  1. get event from Grpc service
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */

public class GatewayServiceManager {

  HelixGatewayServiceService _helixGatewayServiceService;

  HelixGatewayServiceProcessor _helixGatewayServiceProcessor;

  Map<String, HelixGatewayService> _helixGatewayServiceMap;

  // TODO: add thread pool for init
  // single thread tp for update

  public enum EventType {
    CONNECT,    // init connection to gateway service
    UPDATE,  // update state transition result
    DISCONNECT // shutdown connection to gateway service.
  }

  public class GateWayServiceEvent {
    // event type
    EventType eventType;
    // event data
    String clusterName;
    String participantName;

    // todo: add more fields
  }

  public GatewayServiceManager() {
    _helixGatewayServiceMap = new ConcurrentHashMap<>();
  }

  public AtomicBoolean sendTransitionRequestToApplicationInstance() {

    return null;
  }

  public void updateShardState() {

  }

  public void newParticipantConnecting() {

  }

  public void participantDisconnected() {

  }
}
