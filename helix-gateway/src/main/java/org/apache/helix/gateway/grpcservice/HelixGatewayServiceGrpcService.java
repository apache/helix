package org.apache.helix.gateway.grpcservice;

import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.gateway.service.GatewayServiceEvent;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.service.HelixGatewayServiceProcessor;
import org.apache.helix.gateway.util.PerKeyLockRegistry;
import org.apache.helix.gateway.util.StateTransitionMessageTranslateUtil;
import proto.org.apache.helix.gateway.HelixGatewayServiceGrpc;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardState;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage;


/**
 * Helix Gateway Service GRPC UI implementation.
 */
public class HelixGatewayServiceGrpcService extends HelixGatewayServiceGrpc.HelixGatewayServiceImplBase
    implements HelixGatewayServiceProcessor {

  // Map to store the observer for each instance
  private final Map<String, StreamObserver<TransitionMessage>> _observerMap = new HashMap<>();
  // A reverse map to store the instance name for each observer. It is used to find the instance when connection is closed.
  private final Map<StreamObserver<TransitionMessage>, Pair<String, String>> _reversedObserverMap = new HashMap<>();

  private final GatewayServiceManager _manager;

  // A fine grain lock register on instance level
  private final PerKeyLockRegistry _lockRegistry;

  public HelixGatewayServiceGrpcService(GatewayServiceManager manager) {
    _manager = manager;
    _lockRegistry = new PerKeyLockRegistry();
  }

  /**
   * Grpc service end pint.
   * Application instances Report the state of the shard or result of transition request to the gateway service.
   * @param responseObserver
   * @return
   */
  @Override
  public StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage> report(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {

    return new StreamObserver<ShardStateMessage>() {

      @Override
      public void onNext(ShardStateMessage request) {
        if (request.hasShardState()) {
          ShardState shardState = request.getShardState();
          updateObserver(shardState.getInstanceName(), shardState.getClusterName(), responseObserver);
        }
        _manager.newGatewayServiceEvent(StateTransitionMessageTranslateUtil.translateShardStateMessageToEvent(request));
      }

      @Override
      public void onError(Throwable t) {
        onClientClose(responseObserver);
      }

      @Override
      public void onCompleted() {
        onClientClose(responseObserver);
      }
    };
  }

  /**
   * Send state transition message to the instance.
   * The instance must already have established a connection to the gateway service.
   * @param instanceName
   * @return
   */
  @Override
  public boolean sendStateTransitionMessage(String instanceName) {
    StreamObserver<TransitionMessage> observer;
    observer = _observerMap.get(instanceName);
    if (observer != null) {
      observer.onNext(StateTransitionMessageTranslateUtil.translateSTMsgToTransitionMessage());
    }
    return true;
  }

  private void updateObserver(String instanceName, String clusterName,
      StreamObserver<TransitionMessage> streamObserver) {
    _lockRegistry.withLock(instanceName, () -> {
      _observerMap.put(instanceName, streamObserver);
      _reversedObserverMap.put(streamObserver, new ImmutablePair<>(instanceName, clusterName));
    });
  }

  private void onClientClose(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {
    String instanceName;
    String clusterName;
    Pair<String, String> instanceInfo = _reversedObserverMap.get(responseObserver);
    clusterName = instanceInfo.getRight();
    instanceName = instanceInfo.getLeft();

    if (instanceName == null || clusterName == null) {
      // TODO: log error;
      return;
    }
    GatewayServiceEvent event =
        StateTransitionMessageTranslateUtil.translateClientCloseToEvent(clusterName, instanceName);
    _manager.newGatewayServiceEvent(event);
    _lockRegistry.withLock(instanceName, () -> {
      _reversedObserverMap.remove(responseObserver);
      _observerMap.remove(instanceName);
      _lockRegistry.removeLock(instanceName);
    });
  }
}
