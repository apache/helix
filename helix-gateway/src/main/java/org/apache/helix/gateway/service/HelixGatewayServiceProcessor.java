package org.apache.helix.gateway.service;

import java.util.concurrent.ConcurrentHashMap;
import io.grpc.stub.StreamObserver;
import java.util.Map;


/**
 * Translate from/to GRPC function call
 */
public interface HelixGatewayServiceProcessor {

  public boolean sendStateTransitionMessage();

  public void pushEventToManager();
}
