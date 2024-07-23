package org.apache.helix.gateway.grpcservice;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.gateway.service.GatewayServiceManager;
import org.apache.helix.gateway.service.HelixGatewayServiceProcessor;
import proto.org.apache.helix.gateway.*;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.*;

import java.util.Map;


/**
 * Helix Gateway Service GRPC UI implementation.
 */
public class HelixGatewayServiceService extends HelixGatewayServiceGrpc.HelixGatewayServiceImplBase
    implements HelixGatewayServiceProcessor {

  Map<String, StreamObserver<TransitionMessage>> _observerMap =
      new ConcurrentHashMap<String, StreamObserver<TransitionMessage>>();

  @Override
  public StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage> report(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {

    return new StreamObserver<ShardStateMessage>() {

      @Override
      public void onNext(ShardStateMessage request) {
        // called when a client sends a message
        //....
        String instanceName = request.getInstanceName();
        if (!_observerMap.containsValue(instanceName)) {
          // update state map
          updateObserver(instanceName, responseObserver);
        }
        // process the message
      }

      @Override
      public void onError(Throwable t) {
        // called when a client sends an error
        //....
      }

      @Override
      public void onCompleted() {
        // called when the client completes
        //....
      }
    };
  }

  @Override
  public boolean sendStateTransitionMessage(String instanceName) {
    return false;
  }

  @Override
  public void sendEventToManager(GatewayServiceManager.GateWayServiceEvent event) {

  }

  public void updateObserver(String instanceName, StreamObserver<TransitionMessage> streamObserver) {
    _observerMap.put(instanceName, streamObserver);
  }
}
