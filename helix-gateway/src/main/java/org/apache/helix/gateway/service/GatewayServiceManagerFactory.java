package org.apache.helix.gateway.service;

/**
 * Factory class to create GatewayServiceManager
 */
public class GatewayServiceManagerFactory {

  public GatewayServiceManager createGatewayServiceManager() {
    return new GatewayServiceManager();
  }
}
