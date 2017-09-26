package org.apache.helix.api.config;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.map.ObjectWriter;

public class StateTransitionThrottleConfig {
  private static final Logger logger =
      Logger.getLogger(StateTransitionThrottleConfig.class.getName());

  private enum ConfigProperty {
    CONFIG_TYPE,
    REBALANCE_TYPE,
    THROTTLE_SCOPE
  }

  public enum ThrottleScope {
    CLUSTER,
    RESOURCE,
    INSTANCE,
    PARTITION
  }

  public enum RebalanceType {
    LOAD_BALANCE,
    RECOVERY_BALANCE,
    ANY
  }

  public static class StateTransitionType {
    final static String ANY_STATE = "*";
    final static String FROM_KEY = "from";
    final static String TO_KEY = "to";
    String _fromState;
    String _toState;

    StateTransitionType(String fromState, String toState) {
      _fromState = fromState;
      _toState = toState;
    }

    @Override
    public String toString() {
      return FROM_KEY + "." + _fromState + "." + TO_KEY + "." + _toState;
    }

    public static StateTransitionType parseFromString(String stateTransTypeStr) {
      String states[] = stateTransTypeStr.split(".");
      if (states.length < 4 || !states[0].equalsIgnoreCase(FROM_KEY) || !states[2]
          .equalsIgnoreCase(TO_KEY)) {
        return null;
      }
      return new StateTransitionType(states[1], states[3]);
    }
  }

  private ThrottleScope _throttleScope;
  private RebalanceType _rebalanceType;
  private Map<StateTransitionType, Long> _maxPendingStateTransitionMap;

  public StateTransitionThrottleConfig(RebalanceType rebalanceType, ThrottleScope throttleScope) {
    _rebalanceType = rebalanceType;
    _throttleScope = throttleScope;
    _maxPendingStateTransitionMap = new HashMap<StateTransitionType, Long>();
  }

  /**
   * Add a max pending transition from given from state to the specified to state.
   *
   * @param fromState
   * @param toState
   * @param maxPendingStateTransition
   * @return
   */
  public StateTransitionThrottleConfig addThrottle(String fromState, String toState,
      long maxPendingStateTransition) {
    _maxPendingStateTransitionMap
        .put(new StateTransitionType(fromState, toState), maxPendingStateTransition);
    return this;
  }

  /**
   * Add a max pending transition from ANY state to ANY state.
   *
   * @param maxPendingStateTransition
   * @return
   */
  public StateTransitionThrottleConfig addThrottle(long maxPendingStateTransition) {
    _maxPendingStateTransitionMap
        .put(new StateTransitionType(StateTransitionType.ANY_STATE, StateTransitionType.ANY_STATE),
            maxPendingStateTransition);
    return this;
  }

  /**
   * Add a max pending transition for a given state transition type.
   *
   * @param stateTransitionType
   * @param maxPendingStateTransition
   * @return
   */
  public StateTransitionThrottleConfig addThrottle(StateTransitionType stateTransitionType,
      long maxPendingStateTransition) {
    _maxPendingStateTransitionMap.put(stateTransitionType, maxPendingStateTransition);
    return this;
  }

  /**
   * Add a max pending transition from ANY state to the specified state.
   *
   * @param toState
   * @param maxPendingStateTransition
   * @return
   */
  public StateTransitionThrottleConfig addThrottleFromAnyState(String toState,
      long maxPendingStateTransition) {
    _maxPendingStateTransitionMap
        .put(new StateTransitionType(StateTransitionType.ANY_STATE, toState),
            maxPendingStateTransition);
    return this;
  }

  /**
   * Add a max pending transition from given state to ANY state.
   *
   * @param fromState
   * @param maxPendingStateTransition
   * @return
   */
  public StateTransitionThrottleConfig addThrottleToAnyState(String fromState,
      long maxPendingStateTransition) {
    _maxPendingStateTransitionMap
        .put(new StateTransitionType(fromState, StateTransitionType.ANY_STATE),
            maxPendingStateTransition);
    return this;
  }

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Generate the JSON String for StateTransitionThrottleConfig.
   *
   * @return Json String for this config.
   */
  public String toJSON() {
    Map<String, String> configsMap = new HashMap<String, String>();

    configsMap.put(ConfigProperty.REBALANCE_TYPE.name(), _rebalanceType.name());
    configsMap.put(ConfigProperty.THROTTLE_SCOPE.name(), _throttleScope.name());

    for (Map.Entry<StateTransitionType, Long> e : _maxPendingStateTransitionMap.entrySet()) {
      configsMap.put(e.getKey().toString(), String.valueOf(e.getValue()));
    }

    String jsonStr = null;
    try {
      ObjectWriter objectWriter = OBJECT_MAPPER.writer();
      jsonStr = objectWriter.writeValueAsString(configsMap);
    } catch (IOException e) {
      logger.error("Failed to convert config map to JSON object! " + configsMap);
    }

    return jsonStr;
  }

  /**
   * Instantiate a throttle config from a config JSON string.
   *
   * @param configJsonStr
   * @return StateTransitionThrottleConfig or null if the given configs map is not a valid StateTransitionThrottleConfig.
   */
  public static StateTransitionThrottleConfig fromJSON(String configJsonStr) {
    StateTransitionThrottleConfig throttleConfig = null;
    try {
      ObjectReader objectReader = OBJECT_MAPPER.reader(Map.class);
      Map<String, String> configsMap = objectReader.readValue(configJsonStr);
      throttleConfig = fromConfigMap(configsMap);
    } catch (IOException e) {
      logger.error("Failed to convert JSON string to config map! " + configJsonStr);
    }

    return throttleConfig;
  }


  /**
   * Instantiate a throttle config from a config map
   *
   * @param configsMap
   * @return StateTransitionThrottleConfig or null if the given configs map is not a valid StateTransitionThrottleConfig.
   */
  public static StateTransitionThrottleConfig fromConfigMap(Map<String, String> configsMap) {
    if (!configsMap.containsKey(ConfigProperty.REBALANCE_TYPE.name()) ||
        !configsMap.containsKey(ConfigProperty.THROTTLE_SCOPE.name())) {
      // not a valid StateTransitionThrottleConfig
      return null;
    }

    StateTransitionThrottleConfig config;
    try {
      RebalanceType rebalanceType =
          RebalanceType.valueOf(configsMap.get(ConfigProperty.REBALANCE_TYPE.name()));
      ThrottleScope throttleScope =
          ThrottleScope.valueOf(configsMap.get(ConfigProperty.THROTTLE_SCOPE.name()));
      config = new StateTransitionThrottleConfig(rebalanceType, throttleScope);
    } catch (IllegalArgumentException ex) {
      return null;
    }

    for (String configKey : configsMap.keySet()) {
      StateTransitionType transitionType = StateTransitionType.parseFromString(configKey);
      if (transitionType != null) {
        try {
          long value = Long.valueOf(configsMap.get(configKey));
          config.addThrottle(transitionType, value);
        } catch (NumberFormatException ex) {
          // ignore the config item with invalid number.
          logger.warn(String.format("Invalid config entry, key=%s, value=%s", configKey,
              configsMap.get(configKey)));
        }
      }
    }

    return config;
  }
}
