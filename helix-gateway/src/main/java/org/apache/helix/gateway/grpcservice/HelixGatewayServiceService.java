package org.apache.helix.gateway.grpcservice;

import io.grpc.stub.StreamObserver;
import org.apache.helix.gateway.service.HelixGatewayServiceProcessor;
import proto.org.apache.helix.gateway.*;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.*;


public class HelixGatewayServiceService extends HelixGatewayServiceGrpc.HelixGatewayServiceImplBase
    implements HelixGatewayServiceProcessor {

  @Override
  public StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage> report(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {

    return new StreamObserver<ShardStateMessage>() {

      @Override
      public void onNext(ShardStateMessage request) {
        // called when a client sends a message
        //....
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
  public boolean sendStateTransitionMessage() {
    return false;
  }

  @Override
  public void pushEventToManager() {

  }
}
