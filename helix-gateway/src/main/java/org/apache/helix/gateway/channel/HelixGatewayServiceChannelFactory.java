package org.apache.helix.gateway.channel;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.lang3.NotImplementedException;
import org.apache.helix.gateway.api.service.HelixGatewayServiceChannel;
import org.apache.helix.gateway.service.GatewayServiceManager;


public class HelixGatewayServiceChannelFactory {

  public static HelixGatewayServiceChannel createServiceChannel(GatewayServiceChannelConfig config,
      GatewayServiceManager manager) {

    if (config.getChannelMode() == GatewayServiceChannelConfig.ChannelMode.PUSH_MODE) {
      if (config.getParticipantConnectionChannelType() == GatewayServiceChannelConfig.ChannelType.GRPC_SERVER) {
        return new HelixGatewayServiceGrpcService(manager, config);
      }
    } else {
      return new HelixGatewayServicePollModeChannel(config);
    }
    throw new IllegalArgumentException(
        "Unsupported channel mode and type combination: " + config.getChannelMode() + " , "
            + config.getParticipantConnectionChannelType());
  }
}
