package org.apache.helix.controller.stages;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.helix.api.config.StateTransitionThrottleConfig;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.Message;
import org.apache.helix.model.StateModelDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MsgRecordingPerReplicaThrottleStage extends PerReplicaThrottleStage {
  private static final Logger logger =
      LoggerFactory.getLogger(MsgRecordingPerReplicaThrottleStage.class.getName());

  private List<Message> _throttledRecoveryMsg = new ArrayList<>();
  private List<Message> _throttledLoadMsg = new ArrayList<>();

  protected void applyThrottling(String resourceName,
      StateTransitionThrottleController throttleController, StateModelDefinition stateModelDef,
      boolean onlyDownwardLoadBalance, List<Message> messages,
      Set<Message> throttledMessages, StateTransitionThrottleConfig.RebalanceType rebalanceType) {
    Set<Message> middleThrottledMessages = new HashSet<>();
    super.applyThrottling(resourceName, throttleController, stateModelDef,
        onlyDownwardLoadBalance, messages, middleThrottledMessages, rebalanceType);

    if (rebalanceType == StateTransitionThrottleConfig.RebalanceType.RECOVERY_BALANCE) {
      _throttledRecoveryMsg.addAll(middleThrottledMessages);
    } else if (rebalanceType == StateTransitionThrottleConfig.RebalanceType.LOAD_BALANCE) {
      _throttledLoadMsg.addAll(middleThrottledMessages);
    }
    throttledMessages.addAll(middleThrottledMessages);
  }

  public List<Message> getRecoveryThrottledMessages() {
    return _throttledRecoveryMsg;
  }

  public List<Message> getLoadThrottledMessages() {
    return _throttledLoadMsg;
  }
}
