package org.apache.helix.gateway.service;

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

import java.util.List;
import java.util.Map;
import org.apache.helix.gateway.api.constant.GatewayServiceEventType;


/**
 * Event representing message reported by clients to Helix Gateway Service.
 */
public class GatewayServiceEvent {
  // event type
  private GatewayServiceEventType _eventType;
  // event data
  private String _clusterName;
  private String _instanceName;
  // A map where client reports the state of each shard upon connection
  private Map<String, Map<String, String>> _shardStateMap;
  // result for state transition request
  private List<StateTransitionResult> _stateTransitionResult;

  public static class StateTransitionResult {
    private String stateTransitionId;
    private boolean isSuccess;
    private String shardState;

    public StateTransitionResult(String stateTransitionId, boolean isSuccess, String shardState) {
      this.stateTransitionId = stateTransitionId;
      this.isSuccess = isSuccess;
      this.shardState = shardState;
    }

    public String getStateTransitionId() {
      return stateTransitionId;
    }
    public boolean getIsSuccess() {
      return isSuccess;
    }
    public String getShardState() {
      return shardState;
    }
  }

  private GatewayServiceEvent(GatewayServiceEventType eventType, String clusterName, String instanceName,
      Map<String, Map<String, String>> shardStateMap, List<StateTransitionResult> stateTransitionStatusMap) {
    _eventType = eventType;
    _clusterName = clusterName;
    _instanceName = instanceName;
    _shardStateMap = shardStateMap;
    _stateTransitionResult = stateTransitionStatusMap;
  }

  public GatewayServiceEventType getEventType() {
    return _eventType;
  }
  public String getClusterName() {
    return _clusterName;
  }
  public String getInstanceName() {
    return _instanceName;
  }
  public Map<String, Map<String, String>> getShardStateMap() {
    return _shardStateMap;
  }
  public List<StateTransitionResult> getStateTransitionResult() {
    return _stateTransitionResult;
  }


  public static class GateWayServiceEventBuilder {
    private GatewayServiceEventType _eventType;
    private String _clusterName;
    private String _instanceName;
    private Map<String, Map<String, String>> _shardStateMap;
    private List<StateTransitionResult> _stateTransitionResult;

    public GateWayServiceEventBuilder(GatewayServiceEventType eventType) {
      this._eventType = eventType;
    }

    public GateWayServiceEventBuilder setClusterName(String clusterName) {
      this._clusterName = clusterName;
      return this;
    }

    public GateWayServiceEventBuilder setParticipantName(String instanceName) {
      this._instanceName = instanceName;
      return this;
    }

    public GateWayServiceEventBuilder setShardStateMap(Map<String, Map<String, String>> shardStateMap) {
      this._shardStateMap = shardStateMap;
      return this;
    }

    public GateWayServiceEventBuilder setStateTransitionStatusMap(
        List<StateTransitionResult> stateTransitionStatusMap) {
      this._stateTransitionResult = stateTransitionStatusMap;
      return this;
    }

    public GatewayServiceEvent build() {
      return new GatewayServiceEvent(_eventType, _clusterName, _instanceName, _shardStateMap, _stateTransitionResult);
    }
  }
}
