package org.apache.helix.task;

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

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.helix.task.beans.ScheduleBean;
import org.apache.log4j.Logger;

/**
 * Configuration for scheduling both one-time and recurring workflows in Helix
 */
public class ScheduleConfig {
  private static final Logger LOG = Logger.getLogger(ScheduleConfig.class);

  /** Enforce that a workflow can recur at most once per minute */
  private static final long MIN_RECURRENCE_MILLIS = 60 * 1000;

  private final Date _startTime;
  private final TimeUnit _recurUnit;
  private final Long _recurInterval;

  private ScheduleConfig(Date startTime, TimeUnit recurUnit, Long recurInterval) {
    _startTime = startTime;
    _recurUnit = recurUnit;
    _recurInterval = recurInterval;
  }

  /**
   * When the workflow should be started
   * @return Date object representing the start time
   */
  public Date getStartTime() {
    return _startTime;
  }

  /**
   * The unit of the recurrence interval if this is a recurring workflow
   * @return the recurrence interval unit, or null if this workflow is a one-time workflow
   */
  public TimeUnit getRecurrenceUnit() {
    return _recurUnit;
  }

  /**
   * The magnitude of the recurrence interval if this is a recurring task
   * @return the recurrence interval magnitude, or null if this workflow is a one-time workflow
   */
  public Long getRecurrenceInterval() {
    return _recurInterval;
  }

  /**
   * Check if this workflow is recurring
   * @return true if recurring, false if one-time
   */
  public boolean isRecurring() {
    return _recurUnit != null && _recurInterval != null;
  }

  /**
   * Check if the configured schedule is valid given these constraints:
   * <ul>
   * <li>All workflows must have a start time</li>
   * <li>Recurrence unit and interval must both be present if either is present</li>
   * <li>Recurring workflows must have a positive interval magnitude</li>
   * <li>Intervals must be at least one minute</li>
   * </ul>
   * @return true if valid, false if invalid
   */
  public boolean isValid() {
    // All schedules must have a start time even if they are recurring
    if (_startTime == null) {
      LOG.error("All schedules must have a start time!");
      return false;
    }

    // Recurrence properties must both either be present or absent
    if ((_recurUnit == null && _recurInterval != null)
        || (_recurUnit != null && _recurInterval == null)) {
      LOG.error("Recurrence interval and unit must either both be present or both be absent");
      return false;
    }

    // Only positive recurrence intervals are allowed if present
    if (_recurInterval != null && _recurInterval <= 0) {
      LOG.error("Recurrence interval must be positive");
      return false;
    }

    // Enforce minimum interval length
    if (_recurUnit != null) {
      long converted = _recurUnit.toMillis(_recurInterval);
      if (converted < MIN_RECURRENCE_MILLIS) {
        LOG.error("Recurrence must be at least " + MIN_RECURRENCE_MILLIS + " ms");
        return false;
      }
    }
    return true;
  }

  /**
   * Create this configuration from a serialized bean
   * @param bean flat configuration of the schedule
   * @return instantiated ScheduleConfig
   */
  public static ScheduleConfig from(ScheduleBean bean) {
    return new ScheduleConfig(bean.startTime, bean.recurUnit, bean.recurInterval);
  }

  /**
   * Create a schedule for a workflow that runs once at a specified time
   * @param startTime the time to start the workflow
   * @return instantiated ScheduleConfig
   */
  public static ScheduleConfig oneTimeDelayedStart(Date startTime) {
    return new ScheduleConfig(startTime, null, null);
  }

  /**
   * Create a schedule for a recurring workflow that should start immediately
   * @param recurUnit the unit of the recurrence interval
   * @param recurInterval the magnitude of the recurrence interval
   * @return instantiated ScheduleConfig
   */
  public static ScheduleConfig recurringFromNow(TimeUnit recurUnit, long recurInterval) {
    return new ScheduleConfig(new Date(), recurUnit, recurInterval);
  }

  /**
   * Create a schedule for a recurring workflow that should start at a specific time
   * @param startTime the time to start the workflow the first time, or null if now
   * @param recurUnit the unit of the recurrence interval
   * @param recurInterval the magnitude of the recurrence interval
   * @return instantiated ScheduleConfig
   */
  public static ScheduleConfig recurringFromDate(Date startTime, TimeUnit recurUnit,
      long recurInterval) {
    if (startTime == null) {
      startTime = new Date();
    }
    return new ScheduleConfig(startTime, recurUnit, recurInterval);
  }
}
