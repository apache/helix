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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.helix.HelixException;
import org.apache.helix.task.beans.WorkflowBean;
import org.apache.helix.HelixProperty;
import org.apache.log4j.Logger;

/**
 * Provides a typed interface to workflow level configurations. Validates the configurations.
 */
// TODO: extends WorkflowConfig from ResourceConfig
public class  WorkflowConfig {
  private static final Logger LOG = Logger.getLogger(WorkflowConfig.class);

  /**
   * Do not use these values directly, always use the getters/setters
   * in WorkflowConfig and WorkflowConfig.Builder.
   *
   * For back-compatible, this class will be left for public for a while,
   * but it will be change to protected in future major release.
   */
  public enum WorkflowConfigProperty {
    Dag,
    ParallelJobs,
    TargetState,
    Expiry,
    StartTime,
    RecurrenceUnit,
    RecurrenceInterval,
    Terminable,
    FailureThreshold,
    /* this is for non-terminable workflow. */
    capacity,
    WorkflowType,
    IsJobQueue
  }

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
  private final int _capacity;
  private final String _workflowType;
  private final boolean _isJobQueue;

  protected WorkflowConfig(JobDag jobDag, int parallelJobs, TargetState targetState, long expiry,
      int failureThreshold, boolean terminable, ScheduleConfig scheduleConfig, int capacity,
      String workflowType, boolean isJobQueue) {
    _jobDag = jobDag;
    _parallelJobs = parallelJobs;
    _targetState = targetState;
    _expiry = expiry;
    _failureThreshold = failureThreshold;
    _terminable = terminable;
    _scheduleConfig = scheduleConfig;
    _capacity = capacity;
    _workflowType = workflowType;
    _isJobQueue = isJobQueue;
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

  /**
   * This Failure threshold only works for generic workflow. Will be ignored by JobQueue
   * @return
   */
  public int getFailureThreshold() {
    return _failureThreshold;
  }

  /**
   * Determine the number of jobs that this workflow can accept before rejecting further jobs,
   * this field is only used when a workflow is not terminable.
   * @return queue capacity
   */
  public int getCapacity() { return _capacity; }

  public String getWorkflowType() {
    return _workflowType;
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

  public boolean isJobQueue() {
    return _isJobQueue;
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
      cfgMap.put(WorkflowConfigProperty.Dag.name(), getJobDag().toJson());
    } catch (IOException ex) {
      throw new HelixException("Invalid job dag configuration!", ex);
    }
    cfgMap.put(WorkflowConfigProperty.ParallelJobs.name(), String.valueOf(getParallelJobs()));
    cfgMap.put(WorkflowConfigProperty.Expiry.name(), String.valueOf(getExpiry()));
    cfgMap.put(WorkflowConfigProperty.TargetState.name(), getTargetState().name());
    cfgMap.put(WorkflowConfigProperty.Terminable.name(), String.valueOf(isTerminable()));
    cfgMap.put(WorkflowConfigProperty.IsJobQueue.name(), String.valueOf(isJobQueue()));
    cfgMap.put(WorkflowConfigProperty.FailureThreshold.name(),
        String.valueOf(getFailureThreshold()));

    if (_capacity > 0) {
      cfgMap.put(WorkflowConfigProperty.capacity.name(), String.valueOf(_capacity));
    }

    // Populate schedule if present
    ScheduleConfig scheduleConfig = getScheduleConfig();
    if (scheduleConfig != null) {
      Date startTime = scheduleConfig.getStartTime();
      if (startTime != null) {
        String formattedTime = WorkflowConfig.getDefaultDateFormat().format(startTime);
        cfgMap.put(WorkflowConfigProperty.StartTime.name(), formattedTime);
      }
      if (scheduleConfig.isRecurring()) {
        cfgMap.put(WorkflowConfigProperty.RecurrenceUnit.name(),
            scheduleConfig.getRecurrenceUnit().toString());
        cfgMap.put(WorkflowConfigProperty.RecurrenceInterval.name(),
            scheduleConfig.getRecurrenceInterval().toString());
      }
    }
    if (_workflowType != null) {
      cfgMap.put(WorkflowConfigProperty.WorkflowType.name(), _workflowType);
    }
    return cfgMap;
  }

  /**
   * Get a ScheduleConfig from a workflow config string map
   *
   * @param cfg the string map
   * @return a ScheduleConfig if one exists, otherwise null
   */
  public static ScheduleConfig parseScheduleFromConfigMap(Map<String, String> cfg) {
    // Parse schedule-specific configs, if they exist
    Date startTime = null;
    if (cfg.containsKey(WorkflowConfigProperty.StartTime.name())) {
      try {
        startTime = WorkflowConfig.getDefaultDateFormat()
            .parse(cfg.get(WorkflowConfigProperty.StartTime.name()));
      } catch (ParseException e) {
        LOG.error(
            "Unparseable date " + cfg.get(WorkflowConfigProperty.StartTime.name()),
            e);
        return null;
      }
    }
    if (cfg.containsKey(WorkflowConfigProperty.RecurrenceUnit.name()) && cfg
        .containsKey(WorkflowConfigProperty.RecurrenceInterval.name())) {
      return ScheduleConfig.recurringFromDate(startTime,
          TimeUnit.valueOf(cfg.get(WorkflowConfigProperty.RecurrenceUnit.name())),
          Long.parseLong(
              cfg.get(WorkflowConfigProperty.RecurrenceInterval.name())));
    } else if (startTime != null) {
      return ScheduleConfig.oneTimeDelayedStart(startTime);
    }
    return null;
  }

  public static WorkflowConfig fromHelixProperty(HelixProperty property)
      throws IllegalArgumentException {
    Map<String, String> configs = property.getRecord().getSimpleFields();
    if (!configs.containsKey(WorkflowConfigProperty.Dag.name())) {
      throw new IllegalArgumentException(
          String.format("%s is an invalid WorkflowConfig", property.getId()));
    }
    return Builder.fromMap(configs).build();
  }

  public static class Builder {
    private JobDag _taskDag = JobDag.EMPTY_DAG;
    private int _parallelJobs = 1;
    private TargetState _targetState = TargetState.START;
    private long _expiry = DEFAULT_EXPIRY;
    private int _failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    private boolean _isTerminable = true;
    private int _capacity = Integer.MAX_VALUE;
    private ScheduleConfig _scheduleConfig;
    private String _workflowType;
    private boolean _isJobQueue = false;

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_taskDag, _parallelJobs, _targetState, _expiry, _failureThreshold,
          _isTerminable, _scheduleConfig, _capacity, _workflowType, _isJobQueue);
    }

    public Builder() {}

    public Builder(WorkflowConfig workflowConfig) {
      _taskDag = workflowConfig.getJobDag();
      _parallelJobs = workflowConfig.getParallelJobs();
      _targetState = workflowConfig.getTargetState();
      _expiry = workflowConfig.getExpiry();
      _isTerminable = workflowConfig.isTerminable();
      _scheduleConfig = workflowConfig.getScheduleConfig();
      _capacity = workflowConfig.getCapacity();
      _failureThreshold = workflowConfig.getFailureThreshold();
      _workflowType = workflowConfig.getWorkflowType();
      _isJobQueue = workflowConfig.isJobQueue();
    }

    protected Builder setJobDag(JobDag v) {
      _taskDag = v;
      return this;
    }

    /**
     * This method only applies for JobQueue, will be ignored in generic workflows
     * @param parallelJobs Allowed parallel job numbers
     * @return This builder
     */
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

    /**
     * This method only applies for JobQueue, will be ignored in generic workflows
     * @param capacity The number of capacity
     * @return This builder
     */
    public Builder setCapacity(int capacity) {
      _capacity = capacity;
      return this;
    }

    public Builder setWorkFlowType(String workflowType) {
      _workflowType = workflowType;
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

    protected Builder setJobQueue(boolean isJobQueue) {
      _isJobQueue = isJobQueue;
      return this;
    }

    public static Builder fromMap(Map<String, String> cfg) {
      Builder builder = new Builder();
      builder.setConfigMap(cfg);
      return builder;
    }

    public Builder setConfigMap(Map<String, String> cfg) {
      if (cfg.containsKey(WorkflowConfigProperty.Expiry.name())) {
        setExpiry(Long.parseLong(cfg.get(WorkflowConfigProperty.Expiry.name())));
      }
      if (cfg.containsKey(WorkflowConfigProperty.FailureThreshold.name())) {
        setFailureThreshold(
            Integer.parseInt(cfg.get(WorkflowConfigProperty.FailureThreshold.name())));
      }
      if (cfg.containsKey(WorkflowConfigProperty.Dag.name())) {
        setJobDag(JobDag.fromJson(cfg.get(WorkflowConfigProperty.Dag.name())));
      }
      if (cfg.containsKey(WorkflowConfigProperty.TargetState.name())) {
        setTargetState(TargetState.valueOf(cfg.get(WorkflowConfigProperty.TargetState.name())));
      }
      if (cfg.containsKey(WorkflowConfigProperty.Terminable.name())) {
        setTerminable(Boolean.parseBoolean(cfg.get(WorkflowConfigProperty.Terminable.name())));
      }
      if (cfg.containsKey(WorkflowConfigProperty.ParallelJobs.name())) {
        String value = cfg.get(WorkflowConfigProperty.ParallelJobs.name());
        if (value == null) {
          setParallelJobs(1);
        } else {
          setParallelJobs(Integer.parseInt(value));
        }
      }

      if (cfg.containsKey(WorkflowConfigProperty.capacity.name())) {
        int capacity = Integer.valueOf(cfg.get(WorkflowConfigProperty.capacity.name()));
        if (capacity > 0) {
          setCapacity(capacity);
        }
      }

      if (cfg.containsKey(WorkflowConfigProperty.FailureThreshold.name())) {
        int threshold = Integer.valueOf(cfg.get(WorkflowConfigProperty.FailureThreshold.name()));
        if (threshold >= 0) {
          setFailureThreshold(threshold);
        }
      }

      // Parse schedule-specific configs, if they exist
      ScheduleConfig scheduleConfig = parseScheduleFromConfigMap(cfg);
      if (scheduleConfig != null) {
        setScheduleConfig(scheduleConfig);
      }

      if (cfg.containsKey(WorkflowConfigProperty.WorkflowType.name())) {
        setWorkFlowType(cfg.get(WorkflowConfigProperty.WorkflowType.name()));
      }

      if (cfg.containsKey(WorkflowConfigProperty.IsJobQueue.name())) {
        setJobQueue(Boolean.parseBoolean(cfg.get(WorkflowConfigProperty.IsJobQueue.name())));
      }
      return this;
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
      return _isTerminable;
    }

    public int getCapacity() {
      return _capacity;
    }

    public ScheduleConfig getScheduleConfig() {
      return _scheduleConfig;
    }

    public JobDag getJobDag() {
      return _taskDag;
    }

    public boolean isJobQueue() {
      return _isJobQueue;
    }

    public static Builder from(WorkflowBean workflowBean) {
      WorkflowConfig.Builder b = new WorkflowConfig.Builder();
      if (workflowBean.schedule != null) {
        b.setScheduleConfig(ScheduleConfig.from(workflowBean.schedule));
      }
      b.setExpiry(workflowBean.expiry);

      return b;
    }

    private void validate() {
      if (_expiry < 0) {
        throw new IllegalArgumentException(String
            .format("%s has invalid value %s", WorkflowConfigProperty.Expiry.name(), _expiry));
      } else if (_scheduleConfig != null && !_scheduleConfig.isValid()) {
        throw new IllegalArgumentException(
            "Scheduler configuration is invalid. The configuration must have a start time if it is "
                + "one-time, and it must have a positive interval magnitude if it is recurring");
      }
    }
  }
}
