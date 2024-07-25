package org.apache.helix.gateway.service;

import java.util.List;
import java.util.Map;
import org.apache.helix.gateway.constant.GatewayServiceEventType;


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
    private String stateTransitionStatus;
    private String shardState;

    public StateTransitionResult(String stateTransitionId, String stateTransitionStatus, String shardState) {
      this.stateTransitionId = stateTransitionId;
      this.stateTransitionStatus = stateTransitionStatus;
      this.shardState = shardState;
    }

    public String getStateTransitionId() {
      return stateTransitionId;
    }
    public String getStateTransitionStatus() {
      return stateTransitionStatus;
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
