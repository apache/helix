package org.apache.helix.gateway.service;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.helix.gateway.constant.GatewayServiceEventType;
import org.apache.helix.gateway.grpcservice.HelixGatewayServiceGrpcService;
import org.apache.helix.gateway.util.PerKeyBlockingExecutor;


/**
 * A top layer class that send/receive messages from Grpc end point, and dispatch them to corrsponding gateway services.
 *  1. get event from Grpc service
 *  2. Maintain a gateway service registry, one gateway service maps to one Helix cluster
 *  3. On init connect, create the participant manager
 *  4. For ST reply message, update the tracker
 */

public class GatewayServiceManager {
  public static final int CONNECTION_EVENT_THREAD_POOL_SIZE = 10;
  private final Map<String, HelixGatewayService> _helixGatewayServiceMap;

  // a single thread tp for event processing
  private final ExecutorService _participantStateTransitionResultUpdator;

  // link to grpc service
  private final HelixGatewayServiceGrpcService _grpcService;

  // a per key executor for connection event. All event for the same instance will be executed in sequence.
  // It is used to ensure for each instance, the connect/disconnect event won't start until the previous one is done.
  private final PerKeyBlockingExecutor _connectionEventProcessor;

  public GatewayServiceManager() {
    _helixGatewayServiceMap = new ConcurrentHashMap<>();
    _participantStateTransitionResultUpdator = Executors.newSingleThreadExecutor();
    _grpcService = new HelixGatewayServiceGrpcService(this);
    _connectionEventProcessor =
        new PerKeyBlockingExecutor(CONNECTION_EVENT_THREAD_POOL_SIZE); // todo: make it configurable
  }

  /**
   * send state transition message to application instance
   * @return
   */
  public AtomicBoolean sendTransitionRequestToApplicationInstance() {
    // TODO: add param
    return null;
  }

  /**
   * Process the event from Grpc service
   * @param event
   */
  public void newGatewayServiceEvent(GatewayServiceEvent event) {
    if (event.getEventType().equals(GatewayServiceEventType.UPDATE)) {
      _participantStateTransitionResultUpdator.submit(new shardStateUpdator(event));
    } else {
      _connectionEventProcessor.offerEvent(event.getInstanceName(), new participantConnectionProcessor(event));
    }
  }

  /**
   * Update in memory shard state
   */
  class shardStateUpdator implements Runnable {

    GatewayServiceEvent _event;

    public shardStateUpdator(GatewayServiceEvent event) {
      _event = event;
    }

    @Override
    public void run() {
      HelixGatewayService helixGatewayService = _helixGatewayServiceMap.get(_event.getClusterName());
      if (helixGatewayService == null) {
        // TODO: return error code and throw exception.
        return;
      }
      helixGatewayService.receiveSTResponse();
    }
  }

  /**
   * Create HelixGatewayService instance and register it to the manager.
   * It includes waiting for ZK connection, and also wait for previous LiveInstance to expire.
   */
  class participantConnectionProcessor implements Runnable {
    GatewayServiceEvent _event;

    public participantConnectionProcessor(GatewayServiceEvent event) {
      _event = event;
    }

    @Override
    public void run() {
      HelixGatewayService helixGatewayService;
      _helixGatewayServiceMap.computeIfAbsent(_event.getClusterName(),
          k -> new HelixGatewayService(GatewayServiceManager.this, _event.getClusterName()));
      helixGatewayService = _helixGatewayServiceMap.get(_event.getClusterName());
      if (_event.getEventType().equals(GatewayServiceEventType.CONNECT)) {
        helixGatewayService.registerParticipant();
      } else {
        helixGatewayService.deregisterParticipant(_event.getClusterName(), _event.getInstanceName());
      }
    }
  }

  @VisibleForTesting
  HelixGatewayServiceGrpcService getGrpcService() {
    return _grpcService;
  }

  @VisibleForTesting
  HelixGatewayService getHelixGatewayService(String clusterName) {
    return _helixGatewayServiceMap.get(clusterName);
  }
}
