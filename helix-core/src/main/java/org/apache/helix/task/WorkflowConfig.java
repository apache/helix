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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

/**
 * Provides a typed interface to workflow level configurations. Validates the configurations.
 */
public class WorkflowConfig {
  private static final Logger LOG = Logger.getLogger(WorkflowConfig.class);

  /* Config fields */
  public static final String DAG = "Dag";
  public static final String TARGET_STATE = "TargetState";
  public static final String EXPIRY = "Expiry";
  public static final String START_TIME = "StartTime";
  public static final String RECURRENCE_UNIT = "RecurrenceUnit";
  public static final String RECURRENCE_INTERVAL = "RecurrenceInterval";

  /* Default values */
  public static final long DEFAULT_EXPIRY = 24 * 60 * 60 * 1000;
  public static final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat(
      "MM-dd-yyyy HH:mm:ss");
  static {
    DEFAULT_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
  }

  /* Member variables */
  private JobDag _jobDag;
  private TargetState _targetState;
  private long _expiry;
  private ScheduleConfig _scheduleConfig;

  private WorkflowConfig(JobDag jobDag, TargetState targetState, long expiry,
      ScheduleConfig scheduleConfig) {
    _jobDag = jobDag;
    _targetState = targetState;
    _expiry = expiry;
    _scheduleConfig = scheduleConfig;
  }

  public JobDag getJobDag() {
    return _jobDag;
  }

  public TargetState getTargetState() {
    return _targetState;
  }

  public long getExpiry() {
    return _expiry;
  }

  public ScheduleConfig getScheduleConfig() {
    return _scheduleConfig;
  }

  public static class Builder {
    private JobDag _taskDag = JobDag.EMPTY_DAG;
    private TargetState _targetState = TargetState.START;
    private long _expiry = DEFAULT_EXPIRY;
    private ScheduleConfig _scheduleConfig;

    public Builder() {
      // Nothing to do
    }

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_taskDag, _targetState, _expiry, _scheduleConfig);
    }

    public Builder setTaskDag(JobDag v) {
      _taskDag = v;
      return this;
    }

    public Builder setExpiry(long v) {
      _expiry = v;
      return this;
    }

    public Builder setTargetState(TargetState v) {
      _targetState = v;
      return this;
    }

    public Builder setScheduleConfig(ScheduleConfig scheduleConfig) {
      _scheduleConfig = scheduleConfig;
      return this;
    }

    public static Builder fromMap(Map<String, String> cfg) {
      Builder b = new Builder();

      if (cfg == null) {
        return b;
      }

      if (cfg.containsKey(EXPIRY)) {
        b.setExpiry(Long.parseLong(cfg.get(EXPIRY)));
      }
      if (cfg.containsKey(DAG)) {
        b.setTaskDag(JobDag.fromJson(cfg.get(DAG)));
      }
      if (cfg.containsKey(TARGET_STATE)) {
        b.setTargetState(TargetState.valueOf(cfg.get(TARGET_STATE)));
      }

      // Parse schedule-specific configs, if they exist
      Date startTime = null;
      if (cfg.containsKey(START_TIME)) {
        try {
          startTime = DEFAULT_DATE_FORMAT.parse(cfg.get(START_TIME));
        } catch (ParseException e) {
          LOG.error("Unparseable date " + cfg.get(START_TIME), e);
        }
      }
      if (cfg.containsKey(RECURRENCE_UNIT) && cfg.containsKey(RECURRENCE_INTERVAL)) {
        /*
         * b.setScheduleConfig(ScheduleConfig.recurringFromDate(startTime,
         * TimeUnit.valueOf(cfg.get(RECURRENCE_UNIT)),
         * Long.parseLong(cfg.get(RECURRENCE_INTERVAL))));
         */
      } else if (startTime != null) {
        b.setScheduleConfig(ScheduleConfig.oneTimeDelayedStart(startTime));
      }
      return b;
    }

    private void validate() {
      if (_expiry < 0) {
        throw new IllegalArgumentException(
            String.format("%s has invalid value %s", EXPIRY, _expiry));
      } else if (_scheduleConfig != null && !_scheduleConfig.isValid()) {
        throw new IllegalArgumentException(
            "Scheduler configuration is invalid. The configuration must have a start time if it is "
                + "one-time, and it must have a positive interval magnitude if it is recurring");
      }
    }
  }

}
