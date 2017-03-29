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
    THROTTLE_SCOPE,
    MAX_PARTITION_IN_TRANSITION
  }

  public enum ThrottleScope {
    CLUSTER,
    RESOURCE,
    INSTANCE
  }

  public enum RebalanceType {
    LOAD_BALANCE,
    RECOVERY_BALANCE,
    ANY,
    NONE
  }

  RebalanceType _rebalanceType;
  ThrottleScope _throttleScope;
  Long _maxPartitionInTransition;

  public StateTransitionThrottleConfig(RebalanceType rebalanceType,
      ThrottleScope throttleScope, long maxPartitionInTransition) {
    _rebalanceType = rebalanceType;
    _throttleScope = throttleScope;
    _maxPartitionInTransition = maxPartitionInTransition;
  }

  public RebalanceType getRebalanceType() {
    return _rebalanceType;
  }

  public ThrottleScope getThrottleScope() {
    return _throttleScope;
  }

  public Long getMaxPartitionInTransition() {
    return _maxPartitionInTransition;
  }

  private final static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Generate the JSON String for StateTransitionThrottleConfig.
   *
   * @return Json String for this config.
   */
  public String toJSON() {
    Map<String, String> configMap = new HashMap<String, String>();
    configMap.put(ConfigProperty.REBALANCE_TYPE.name(), _rebalanceType.name());
    configMap.put(ConfigProperty.THROTTLE_SCOPE.name(), _throttleScope.name());
    configMap.put(ConfigProperty.MAX_PARTITION_IN_TRANSITION.name(),
        String.valueOf(_maxPartitionInTransition));

    String jsonStr = null;
    try {
      ObjectWriter objectWriter = OBJECT_MAPPER.writer();
      jsonStr = objectWriter.writeValueAsString(configMap);
    } catch (IOException e) {
      logger.error("Failed to convert config map to JSON object! " + configMap);
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
   *
   * @return StateTransitionThrottleConfig or null if the given configs map is not a valid
   * StateTransitionThrottleConfig.
   */
  public static StateTransitionThrottleConfig fromConfigMap(Map<String, String> configsMap) {
    if (!configsMap.containsKey(ConfigProperty.REBALANCE_TYPE.name()) || !configsMap
        .containsKey(ConfigProperty.THROTTLE_SCOPE.name())) {
      // not a valid StateTransitionThrottleConfig
      return null;
    }

    StateTransitionThrottleConfig config;
    try {
      RebalanceType rebalanceType =
          RebalanceType.valueOf(configsMap.get(ConfigProperty.REBALANCE_TYPE.name()));
      ThrottleScope throttleScope =
          ThrottleScope.valueOf(configsMap.get(ConfigProperty.THROTTLE_SCOPE.name()));
      Long maxPartition =
          Long.valueOf(configsMap.get(ConfigProperty.MAX_PARTITION_IN_TRANSITION.name()));
      config = new StateTransitionThrottleConfig(rebalanceType, throttleScope, maxPartition);
    } catch (IllegalArgumentException ex) {
      return null;
    }

    return config;
  }
}
