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

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.ZNRecord;

public class StateTransitionTimeoutConfig {
  public enum StateTransitionTimeoutProperty {
    /**
     * The timeout for a state transition
     */
    TIMEOUT
  }

  private final String _resource;
  private final Map<String, String> _timeoutMap;

  public StateTransitionTimeoutConfig(ZNRecord record) {
    _resource = record.getId();
    if (record.getMapFields().containsKey(StateTransitionTimeoutProperty.TIMEOUT.name())) {
      _timeoutMap = record.getMapField(StateTransitionTimeoutProperty.TIMEOUT.name());
    } else {
      _timeoutMap = new HashMap<String, String>();
    }
  }

  /**
   * Set state transition timeout for given resource.
   * Does not apply for Workflow and Job
   * @param from          The from state
   * @param to            The to state
   * @param timeout       The timeout in miliseconds
   */
  public void setStateTransitionTimeout(String from, String to, int timeout) {
    setStateTransitionTimeout(null, from, to, timeout);
  }

  /**
   * Set state transition timeout for general condition.
   * Does not apply for Workflow and Job
   * @param partitionName The partition prefer to time out
   * @param from          The from state
   * @param to            The to state
   * @param timeout       The timeout in miliseconds
   */
  private void setStateTransitionTimeout(String partitionName, String from, String to,
      int timeout) {
    if (partitionName != null) {
      _timeoutMap.put(partitionName, String.valueOf(timeout));
    } else {
      _timeoutMap.put(String.format("%s.%s", from, to), String.valueOf(timeout));
    }
  }

  /**
   * Get state transition time out for given partition.
   * Does not apply for Workflow and Job
   * @param partitionName The partition prefer to time out
   * @param from          The from state
   * @param to            The to state
   * @return              The timeout in miliseconds. Or -1 if there is no timeout matched up.
   */
  public int getStateTransitionTimeout(String partitionName, String from, String to) {
    if (partitionName != null && _timeoutMap.containsKey(partitionName)) {
      return Integer.parseInt(_timeoutMap.get(partitionName));
    } else if (_timeoutMap.containsKey(String.format("%s.%s", from, to))) {
      return Integer.parseInt(_timeoutMap.get(String.format("%s.%s", from, to)));
    } else if (_timeoutMap.containsKey(String.format("*.%s", to))) {
      return Integer.parseInt(_timeoutMap.get(String.format("*.%s", to)));
    } else if (_timeoutMap.containsKey(String.format("%s.*", from))) {
      return Integer.parseInt(_timeoutMap.get(String.format("%s.*", from)));
    } else if (_timeoutMap.containsKey("*.*")) {
      return Integer.parseInt(_timeoutMap.get("*.*"));
    }
    return -1;
  }

  /**
   * Get state transition time out for given partition.
   * Does not apply for Workflow and Job
   * @param from          The from state
   * @param to            The to state
   * @return              The timeout in miliseconds. Or -1 if there is no timeout matched up.
   */
  public int getStateTransitionTimeout(String from, String to) {
    return getStateTransitionTimeout(null, from, to);
  }

  public Map<String, String> getTimeoutMap() {
    return _timeoutMap;
  }

  public String getResource() {
    return _resource;
  }

  /**
   * Get StateTransitionTimeoutConfig from ZNRecord instead of creating a new
   * StateTransitionTimeoutConfig object.
   * @param record The ZNRecord to extract StateTransitionTimeoutConfig
   * @return       A StateTransitionTimeoutConfig if ZNRecord contains StateTransitionTimeoutConfig
   *               setting.
   */
  public static StateTransitionTimeoutConfig fromRecord(ZNRecord record) {
    return record.getMapFields()
        .containsKey(StateTransitionTimeoutConfig.StateTransitionTimeoutProperty.TIMEOUT.name())
        ? new StateTransitionTimeoutConfig(record)
        : null;
  }
}
