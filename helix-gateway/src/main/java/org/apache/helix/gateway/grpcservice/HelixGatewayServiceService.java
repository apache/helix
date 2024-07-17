package org.apache.helix.gateway.grpcservice;

import proto.org.apache.helix.gateway.*;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.*;
import io.grpc.stub.StreamObserver;

public class HelixGatewayServiceService extends HelixGatewayServiceGrpc.HelixGatewayServiceImplBase {

  @Override
  public StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ReplicaStateMessage> report(
      StreamObserver<proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage> responseObserver) {

    return new StreamObserver<ReplicaStateMessage>() {

      @Override
      public void onNext(ReplicaStateMessage request) {
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
}
