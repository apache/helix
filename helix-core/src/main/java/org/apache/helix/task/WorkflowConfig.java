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
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixException;

/**
 * Provides a typed interface to workflow level configurations. Validates the configurations.
 */
public class WorkflowConfig {
  /* Config fields */
  public static final String DAG = "Dag";
  public static final String PARALLEL_JOBS = "ParallelJobs";
  public static final String TARGET_STATE = "TargetState";
  public static final String EXPIRY = "Expiry";
  public static final String START_TIME = "StartTime";
  public static final String RECURRENCE_UNIT = "RecurrenceUnit";
  public static final String RECURRENCE_INTERVAL = "RecurrenceInterval";
  public static final String TERMINABLE = "Terminable";
  public static final String FAILURE_THRESHOLD = "FailureThreshold";

  /* Default values */
  public static final long DEFAULT_EXPIRY = 24 * 60 * 60 * 1000;
  public static final int DEFAULT_FAILURE_THRESHOLD = 0;

  /* Member variables */
  // TODO: jobDag should not be in the workflowConfig.
  private final JobDag _jobDag;

  // _parallelJobs would kind of break the job dependency,
  // e.g: if job1 -> job2, but _parallelJobs == 2,
  // then job1 and job2 could be scheduled at the same time
  private final int _parallelJobs;
  private final TargetState _targetState;
  private final long _expiry;
  private final boolean _terminable;
  private final ScheduleConfig _scheduleConfig;
  private final int _failureThreshold;

  protected WorkflowConfig(JobDag jobDag, int parallelJobs, TargetState targetState, long expiry,
      int failureThreshold, boolean terminable, ScheduleConfig scheduleConfig) {
    _jobDag = jobDag;
    _parallelJobs = parallelJobs;
    _targetState = targetState;
    _expiry = expiry;
    _failureThreshold = failureThreshold;
    _terminable = terminable;
    _scheduleConfig = scheduleConfig;
  }

  public JobDag getJobDag() {
    return _jobDag;
  }

  public int getParallelJobs() {
    return _parallelJobs;
  }

  public TargetState getTargetState() {
    return _targetState;
  }

  public long getExpiry() {
    return _expiry;
  }

  public int getFailureThreshold() {
    return _failureThreshold;
  }

  public boolean isTerminable() {
    return _terminable;
  }

  public ScheduleConfig getScheduleConfig() {
    return _scheduleConfig;
  }

  public boolean isRecurring() {
    return _scheduleConfig != null && _scheduleConfig.isRecurring();
  }

  public static SimpleDateFormat getDefaultDateFormat() {
    SimpleDateFormat defaultDateFormat = new SimpleDateFormat(
        "MM-dd-yyyy HH:mm:ss");
    defaultDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    return defaultDateFormat;
  }

  /**
   * Get the scheduled start time of the workflow.
   *
   * @return start time if the workflow schedule is set, null if no schedule config set.
   */
  public Date getStartTime() {
    // Workflow with non-scheduled config is ready to schedule immediately.
    if (_scheduleConfig == null) {
      return null;
    }

    return _scheduleConfig.getStartTime();
  }

  public Map<String, String> getResourceConfigMap() {
    Map<String, String> cfgMap = new HashMap<String, String>();
    try {
      cfgMap.put(WorkflowConfig.DAG, getJobDag().toJson());
    } catch (IOException ex) {
      throw new HelixException("Invalid job dag configuration!", ex);
    }
    cfgMap.put(WorkflowConfig.PARALLEL_JOBS, String.valueOf(getParallelJobs()));
    cfgMap.put(WorkflowConfig.EXPIRY, String.valueOf(getExpiry()));
    cfgMap.put(WorkflowConfig.TARGET_STATE, getTargetState().name());
    cfgMap.put(WorkflowConfig.TERMINABLE, String.valueOf(isTerminable()));
    cfgMap.put(WorkflowConfig.FAILURE_THRESHOLD, String.valueOf(getFailureThreshold()));

    // Populate schedule if present
    ScheduleConfig scheduleConfig = getScheduleConfig();
    if (scheduleConfig != null) {
      Date startTime = scheduleConfig.getStartTime();
      if (startTime != null) {
        String formattedTime = WorkflowConfig.getDefaultDateFormat().format(startTime);
        cfgMap.put(WorkflowConfig.START_TIME, formattedTime);
      }
      if (scheduleConfig.isRecurring()) {
        cfgMap.put(WorkflowConfig.RECURRENCE_UNIT, scheduleConfig.getRecurrenceUnit().toString());
        cfgMap.put(WorkflowConfig.RECURRENCE_INTERVAL, scheduleConfig.getRecurrenceInterval()
            .toString());
      }
    }
    return cfgMap;
  }

  public static class Builder {
    private JobDag _taskDag = JobDag.EMPTY_DAG;
    private int _parallelJobs = 1;
    private TargetState _targetState = TargetState.START;
    private long _expiry = DEFAULT_EXPIRY;
    private int _failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    private boolean _isTerminable = true;
    private ScheduleConfig _scheduleConfig;

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_taskDag, _parallelJobs, _targetState, _expiry, _failureThreshold,
          _isTerminable, _scheduleConfig);
    }

    public Builder() {}

    public Builder(WorkflowConfig workflowConfig) {
      _taskDag = workflowConfig.getJobDag();
      _parallelJobs = workflowConfig.getParallelJobs();
      _targetState = workflowConfig.getTargetState();
      _expiry = workflowConfig.getExpiry();
      _isTerminable = workflowConfig.isTerminable();
      _scheduleConfig = workflowConfig.getScheduleConfig();
    }

    public Builder setJobDag(JobDag v) {
      _taskDag = v;
      return this;
    }

    public Builder setParallelJobs(int parallelJobs) {
      _parallelJobs = parallelJobs;
      return this;
    }

    public Builder setExpiry(long v, TimeUnit unit) {
      _expiry = unit.toMillis(v);
      return this;
    }

    public Builder setExpiry(long v) {
      _expiry = v;
      return this;
    }

    public Builder setFailureThreshold(int failureThreshold) {
      _failureThreshold = failureThreshold;
      return this;
    }

    public Builder setTerminable(boolean isTerminable) {
      _isTerminable = isTerminable;
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
      if (cfg.containsKey(EXPIRY)) {
        b.setExpiry(Long.parseLong(cfg.get(EXPIRY)));
      }
      if (cfg.containsKey(FAILURE_THRESHOLD)) {
        b.setFailureThreshold(Integer.parseInt(cfg.get(FAILURE_THRESHOLD)));
      }
      if (cfg.containsKey(DAG)) {
        b.setJobDag(JobDag.fromJson(cfg.get(DAG)));
      }
      if (cfg.containsKey(TARGET_STATE)) {
        b.setTargetState(TargetState.valueOf(cfg.get(TARGET_STATE)));
      }
      if (cfg.containsKey(TERMINABLE)) {
        b.setTerminable(Boolean.parseBoolean(cfg.get(TERMINABLE)));
      }
      if (cfg.containsKey(PARALLEL_JOBS)) {
        String value = cfg.get(PARALLEL_JOBS);
        if (value == null) {
          b.setParallelJobs(1);
        } else {
          b.setParallelJobs(Integer.parseInt(value));
        }
      }

      // Parse schedule-specific configs, if they exist
      ScheduleConfig scheduleConfig = TaskUtil.parseScheduleFromConfigMap(cfg);
      if (scheduleConfig != null) {
        b.setScheduleConfig(scheduleConfig);
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
