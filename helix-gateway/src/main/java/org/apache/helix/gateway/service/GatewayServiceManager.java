package org.apache.helix.gateway.service;

import com.google.common.collect.Lists;
import java.util.Map;

import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.gateway.grpcservice.HelixGatewayServiceService;


/**
 * A top layer class that send/receive messages from Grpc end point, and dispatch them to corrsponding gateway services.
 *  1. get event from Handler
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */

public class GatewayServiceManager {

  HelixGatewayServiceService _helixGatewayServiceService;

  HelixGatewayServiceProcessor _helixGatewayServiceProcessor;

  Map<String, HelixGatewayService> _helixGatewayServiceMap;

  // event queue
  // state tracker, call tracker.update

  // tp for init
  // single thread tp for update

  public enum EventType {
    INIT,    // init connection to gateway service
    UPDATE,  // update state transition result
    SHUTDOWN // shutdown connection to gateway service.
  }

  public class GateWayServiceEvent {
    // event type
    EventType eventType;
    // event data
    String clusterName;
    String participantName;
    String resourceName;
    String shardName;
    String shardState;
  }

  Queue<GateWayServiceEvent> _eventQueue;

  public GatewayServiceManager() {
    _helixGatewayServiceMap = new ConcurrentHashMap<>();
    _eventQueue = Lists.newLinkedList();
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
