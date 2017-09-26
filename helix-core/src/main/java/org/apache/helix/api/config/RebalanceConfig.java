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

import org.apache.helix.ZNRecord;
import org.apache.helix.controller.rebalancer.Rebalancer;
import org.apache.helix.task.TaskRebalancer;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Resource's rebalance configurations
 */
public class RebalanceConfig {
  /**
   * Configurable rebalance options of a resource
   */
  public enum RebalanceConfigProperty {
    REBALANCE_DELAY,
    DELAY_REBALANCE_DISABLED,
    REBALANCE_MODE,
    REBALANCER_CLASS_NAME,
    REBALANCE_TIMER_PERIOD,
    REBALANCE_STRATEGY
  }

  /**
   * The mode used for rebalance. FULL_AUTO does both node location calculation and state
   * assignment, SEMI_AUTO only does the latter, and CUSTOMIZED does neither. USER_DEFINED
   * uses a Rebalancer implementation plugged in by the user. TASK designates that a
   * {@link TaskRebalancer} instance should be used to rebalance this resource.
   */
  public enum RebalanceMode {
    FULL_AUTO,
    SEMI_AUTO,
    CUSTOMIZED,
    USER_DEFINED,
    TASK,
    NONE
  }

  private static final int DEFAULT_REBALANCE_DELAY = -1;

  private long _rebalanceDelay = DEFAULT_REBALANCE_DELAY;
  private RebalanceMode _rebalanceMode;
  private String _rebalancerClassName;
  private String _rebalanceStrategy;
  private Boolean _delayRebalanceDisabled;
  private long _rebalanceTimerPeriod = -1;  /* in seconds */

  private static final Logger _logger = Logger.getLogger(RebalanceConfig.class.getName());

  /**
   * Instantiate from an znRecord
   *
   * @param znRecord
   */
  public RebalanceConfig(ZNRecord znRecord) {
    _rebalanceDelay = znRecord.getLongField(RebalanceConfigProperty.REBALANCE_DELAY.name(), -1);
    _rebalanceMode = znRecord
        .getEnumField(RebalanceConfigProperty.REBALANCE_MODE.name(), RebalanceMode.class,
            RebalanceMode.NONE);
    _rebalancerClassName =
        znRecord.getSimpleField(RebalanceConfigProperty.REBALANCER_CLASS_NAME.name());
    _rebalanceStrategy = znRecord.getSimpleField(RebalanceConfigProperty.REBALANCE_STRATEGY.name());
    _delayRebalanceDisabled =
        znRecord.getBooleanField(RebalanceConfigProperty.DELAY_REBALANCE_DISABLED.name(), false);
    _rebalanceTimerPeriod =
        znRecord.getLongField(RebalanceConfigProperty.REBALANCE_TIMER_PERIOD.name(), -1);
  }

  /**
   * Get rebalance delay (in milliseconds), default is -1 is not set.
   * @return
   */
  public long getRebalanceDelay() {
    return _rebalanceDelay;
  }

  /**
   * Set the delay time (in ms) that Helix should move the partition after an instance goes offline.
   * This option only takes effects when delay rebalance is enabled.
   * @param rebalanceDelay
   */
  public void setRebalanceDelay(long rebalanceDelay) {
    this._rebalanceDelay = rebalanceDelay;
  }

  public RebalanceMode getRebalanceMode() {
    return _rebalanceMode;
  }

  public void setRebalanceMode(RebalanceMode rebalanceMode) {
    this._rebalanceMode = rebalanceMode;
  }

  /**
   * Get the name of the user-defined rebalancer associated with this resource
   * @return the rebalancer class name, or null if none is being used
   */
  public String getRebalanceClassName() {
    return _rebalancerClassName;
  }

  /**
   * Define a custom rebalancer that implements {@link Rebalancer}
   * @param rebalancerClassName the name of the custom rebalancing class
   */
  public void setRebalanceClassName(String rebalancerClassName) {
    this._rebalancerClassName = rebalancerClassName;
  }

  /**
   * Get the rebalance strategy for this resource.
   *
   * @return rebalance strategy, or null if not specified.
   */
  public String getRebalanceStrategy() {
    return _rebalanceStrategy;
  }

  /**
   * Specify the strategy for Helix to use to compute the partition-instance assignment,
   * i,e, the custom rebalance strategy that implements {@link org.apache.helix.controller.rebalancer.strategy.RebalanceStrategy}
   *
   * @param rebalanceStrategy
   * @return
   */
  public void setRebalanceStrategy(String rebalanceStrategy) {
    this._rebalanceStrategy = rebalanceStrategy;
  }

  /**
   * Whether the delay rebalance is disabled. By default, it is false.
   * @return
   */
  public Boolean isDelayRebalanceDisabled() {
    return _delayRebalanceDisabled;
  }

  /**
   * If disabled is true, the delayed rebalance time will be ignored.
   * @param delayRebalanceDisabled
   */
  public void setDelayRebalanceDisabled(Boolean delayRebalanceDisabled) {
    this._delayRebalanceDisabled = delayRebalanceDisabled;
  }

  /**
   * Get the frequency with which to rebalance
   * @return the rebalancing timer period
   */
  public long getRebalanceTimerPeriod() {
    return _rebalanceTimerPeriod;
  }

  /**
   * Set the frequency with which to rebalance
   * @param  rebalanceTimerPeriod
   */
  public void setRebalanceTimerPeriod(long rebalanceTimerPeriod) {
    this._rebalanceTimerPeriod = rebalanceTimerPeriod;
  }

  /**
   * Generate the config map for RebalanceConfig.
   *
   * @return
   */
  public Map<String, String> getConfigsMap() {
    Map<String, String> simpleFieldMap = new HashMap<String, String>();

    if (_rebalanceDelay >= 0) {
      simpleFieldMap
          .put(RebalanceConfigProperty.REBALANCE_DELAY.name(), String.valueOf(_rebalanceDelay));
    }
    if (_rebalanceMode != null) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCE_MODE.name(), _rebalanceMode.name());
    }
    if (_rebalancerClassName != null) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCER_CLASS_NAME.name(), _rebalancerClassName);
    }
    if (_rebalanceStrategy != null) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCE_STRATEGY.name(), _rebalanceStrategy);
    }
    if (_delayRebalanceDisabled != null) {
      simpleFieldMap.put(RebalanceConfigProperty.DELAY_REBALANCE_DISABLED.name(),
          String.valueOf(_delayRebalanceDisabled));
    }
    if (_rebalanceTimerPeriod > 0) {
      simpleFieldMap.put(RebalanceConfigProperty.REBALANCE_TIMER_PERIOD.name(),
          String.valueOf(_rebalanceTimerPeriod));
    }

    return simpleFieldMap;
  }

  public boolean isValid() {
    return true;
  }
}

