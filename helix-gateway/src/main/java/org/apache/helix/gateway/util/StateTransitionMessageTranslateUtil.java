package org.apache.helix.gateway.util;

import org.apache.helix.gateway.service.GatewayServiceManager;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.TransitionMessage;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass.ShardStateMessage;


public final class StateTransitionMessageTranslateUtil {

  public static TransitionMessage translateSTMsgToProto() {
    return null;
  }

  public static GatewayServiceManager.GateWayServiceEvent translateProtoToSTMsg(ShardStateMessage message) {
    return null;
  }
}
