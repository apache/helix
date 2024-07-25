package org.apache.helix.gateway.constant;

public enum GatewayServiceEventType {
  CONNECT,    // init connection to gateway service
  UPDATE,  // update state transition result
  DISCONNECT // shutdown connection to gateway service.
}
