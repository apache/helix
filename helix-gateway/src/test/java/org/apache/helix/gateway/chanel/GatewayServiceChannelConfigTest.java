package org.apache.helix.gateway.chanel;

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

import org.apache.helix.gateway.channel.GatewayServiceChannelConfig;
import org.testng.Assert;
import org.testng.annotations.Test;


public class GatewayServiceChannelConfigTest {

  @Test
  public void testGatewayServiceChannelConfigBuilder() {
    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder();

    builder.setChannelMode(GatewayServiceChannelConfig.ChannelMode.PUSH_MODE)
        .setParticipantConnectionChannelType(GatewayServiceChannelConfig.ChannelType.GRPC_SERVER)
        .setShardStateProcessorType(GatewayServiceChannelConfig.ChannelType.GRPC_SERVER)
        .setGrpcServerPort(50051)
        .setServerHeartBeatInterval(30)
        .setMaxAllowedClientHeartBeatInterval(60)
        .setClientTimeout(120)
        .setEnableReflectionService(true)
        .setPollIntervalSec(10);

    GatewayServiceChannelConfig config = builder.build();

    Assert.assertEquals(config.getChannelMode(), GatewayServiceChannelConfig.ChannelMode.PUSH_MODE);
    Assert.assertEquals(config.getParticipantConnectionChannelType(),
        GatewayServiceChannelConfig.ChannelType.GRPC_SERVER);
    Assert.assertEquals(config.getShardStateChannelType(), GatewayServiceChannelConfig.ChannelType.GRPC_SERVER);
    Assert.assertEquals(config.getGrpcServerPort(), 50051);
    Assert.assertEquals(config.getServerHeartBeatInterval(), 30);
    Assert.assertEquals(config.getMaxAllowedClientHeartBeatInterval(), 60);
    Assert.assertEquals(config.getClientTimeout(), 120);
    Assert.assertTrue(config.getEnableReflectionService());
    Assert.assertEquals(config.getPollIntervalSec(), 10);
  }

  @Test
  public void testInvalidConfig() {
    GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder builder =
        new GatewayServiceChannelConfig.GatewayServiceProcessorConfigBuilder();

    builder.setParticipantConnectionChannelType(GatewayServiceChannelConfig.ChannelType.GRPC_SERVER);

    // assert er get an exception
    try {
      builder.build();
      Assert.fail("Should have thrown an exception");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
