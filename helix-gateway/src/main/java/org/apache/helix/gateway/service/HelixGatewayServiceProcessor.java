package org.apache.helix.gateway.service;

import java.util.concurrent.ConcurrentHashMap;
import io.grpc.stub.StreamObserver;
import java.util.Map;


/**
 * Translate from/to GRPC function call
 */
public interface HelixGatewayServiceProcessor {

  public boolean sendStateTransitionMessage(String instanceName);

  public void pushEventToManager(GatewayServiceManager.GateWayServiceEvent event);



}
