package org.apache.helix.gateway.service;

/**
 * Translate from/to GRPC function call to Helix Gateway Service event.
 */
public interface HelixGatewayServiceProcessor {

  public boolean sendStateTransitionMessage( String instanceName);

}
