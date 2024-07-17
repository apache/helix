package org.apache.helix.gateway.service;

import java.util.concurrent.ConcurrentHashMap;
import io.grpc.stub.StreamObserver;
import java.util.Map;


/**
 * Translate from/to GRPC function call
 */
public class HelixGatewayServiceProcessor{

  Map<String, StreamObserver> applicationInstanceMap;

  public HelixGatewayServiceProcessor() {
    applicationInstanceMap = new ConcurrentHashMap<String, StreamObserver>();
  }

  public boolean sendStateTransitionMessage() {
    //streamObserver.onNext(translateSTMsgToProto(msg));
    return true;
  }

  public void pushEventToManager() {

  }



}
