package org.apache.helix.gateway.utils;/*
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

import org.apache.helix.HelixDefinedState;

import org.apache.helix.gateway.participant.HelixGatewayParticipant;
import org.apache.helix.gateway.util.StateTransitionMessageTranslateUtil;
import org.testng.Assert;
import org.testng.annotations.Test;
import proto.org.apache.helix.gateway.HelixGatewayServiceOuterClass;

public class TestStateTransitionMessageTranslateUtil {

  @Test
  public void testTranslateStatesToTransitionType_DeleteShard() {
    String currentState = "ONLINE";
    String toState = HelixDefinedState.DROPPED.name();

    HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType result =
        StateTransitionMessageTranslateUtil.translateStatesToTransitionType(currentState, toState);

    Assert.assertEquals(result,
        HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType.DELETE_SHARD,
        "Expected DELETE_SHARD when transitioning to DROPPED state from a non-DROPPED state.");
  }

  @Test
  public void testTranslateStatesToTransitionType_AddShard() {
    String currentState = HelixGatewayParticipant.UNASSIGNED_STATE;
    String toState = "ONLINE";

    HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType result =
        StateTransitionMessageTranslateUtil.translateStatesToTransitionType(currentState, toState);

    Assert.assertEquals(result,
        HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType.ADD_SHARD,
        "Expected ADD_SHARD when transitioning from DROPPED state to a non-DROPPED state.");
  }

  @Test
  public void testTranslateStatesToTransitionType_ChangeRole() {
    String currentState = "ONLINE";
    String toState = "OFFLINE";

    HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType result =
        StateTransitionMessageTranslateUtil.translateStatesToTransitionType(currentState, toState);

    Assert.assertEquals(result,
        HelixGatewayServiceOuterClass.SingleShardChangeRequest.StateChangeRequestType.CHANGE_ROLE,
        "Expected CHANGE_ROLE when transitioning between non-DROPPED states.");
  }
}
