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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.task.beans.WorkflowBean;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a typed interface to workflow level configurations. Validates the configurations.
 */
public class WorkflowConfig extends ResourceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowConfig.class);

  /**
   * Do not use these values directly, always use the getters/setters
   * in WorkflowConfig and WorkflowConfig.Builder.
   * For back-compatible, this class will be left for public for a while,
   * but it will be change to protected in future major release.
   */
  public enum WorkflowConfigProperty {
    WorkflowID,
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
    JobTypes,
    IsJobQueue,
    JobPurgeInterval,
    /* Allow multiple jobs in this workflow to be assigned to a same instance or not */
    AllowOverlapJobAssignment,
    Timeout
  }

  /* Default values */
  public static final long DEFAULT_EXPIRY = 24 * 60 * 60 * 1000;
  public static final int DEFAULT_FAILURE_THRESHOLD = 0;
  public static final int DEFAULT_PARALLEL_JOBS = 1;
  public static final int DEFAULT_CAPACITY = Integer.MAX_VALUE;
  public static final JobDag DEFAULT_JOB_DAG = JobDag.EMPTY_DAG;
  public static final TargetState DEFAULT_TARGET_STATE = TargetState.START;
  public static final boolean DEFAULT_TERMINABLE = true;
  public static final boolean DEFAULT_JOB_QUEUE = false;
  public static final boolean DEFAULT_MONITOR_DISABLE = true;
  public static final boolean DEFAULT_ALLOW_OVERLAP_JOB_ASSIGNMENT = false;
  protected static final long DEFAULT_JOB_PURGE_INTERVAL = 30 * 60 * 1000; // default 30 minutes
  private JobDag _jobDag;

  public WorkflowConfig(HelixProperty property) {
    super(property.getRecord());
  }

  public WorkflowConfig(WorkflowConfig cfg, String workflowId) {
    this(workflowId, cfg.getJobDag(), cfg.getParallelJobs(), cfg.getTargetState(), cfg.getExpiry(),
        cfg.getFailureThreshold(), cfg.isTerminable(), cfg.getScheduleConfig(), cfg.getCapacity(),
        cfg.getWorkflowType(), cfg.isJobQueue(), cfg.getJobTypes(), cfg.getJobPurgeInterval(),
        cfg.isAllowOverlapJobAssignment(), cfg.getTimeout());
  }

  /* Member variables */
  // TODO: jobDag should not be in the workflowConfig.

  protected WorkflowConfig(String workflowId, JobDag jobDag, int parallelJobs,
      TargetState targetState, long expiry, int failureThreshold, boolean terminable,
      ScheduleConfig scheduleConfig, int capacity, String workflowType, boolean isJobQueue,
      Map<String, String> jobTypes, long purgeInterval, boolean allowOverlapJobAssignment,
      long timeout) {
    super(workflowId);

    putSimpleConfig(WorkflowConfigProperty.WorkflowID.name(), workflowId);
    try {
      putSimpleConfig(WorkflowConfigProperty.Dag.name(), jobDag.toJson());
    } catch (IOException ex) {
      throw new HelixException("Invalid job dag configuration!", ex);
    }
    putSimpleConfig(WorkflowConfigProperty.ParallelJobs.name(), String.valueOf(parallelJobs));
    putSimpleConfig(WorkflowConfigProperty.Expiry.name(), String.valueOf(expiry));
    putSimpleConfig(WorkflowConfigProperty.TargetState.name(), targetState.name());
    putSimpleConfig(WorkflowConfigProperty.Terminable.name(), String.valueOf(terminable));
    putSimpleConfig(WorkflowConfigProperty.IsJobQueue.name(), String.valueOf(isJobQueue));
    putSimpleConfig(WorkflowConfigProperty.FailureThreshold.name(),
        String.valueOf(failureThreshold));
    putSimpleConfig(WorkflowConfigProperty.JobPurgeInterval.name(), String.valueOf(purgeInterval));
    putSimpleConfig(WorkflowConfigProperty.AllowOverlapJobAssignment.name(),
        String.valueOf(allowOverlapJobAssignment));

    if (capacity > 0) {
      putSimpleConfig(WorkflowConfigProperty.capacity.name(), String.valueOf(capacity));
    }

    if (timeout != TaskConstants.DEFAULT_NEVER_TIMEOUT) {
      putSimpleConfig(WorkflowConfigProperty.Timeout.name(), String.valueOf(timeout));
    }

    // Populate schedule if present
    if (scheduleConfig != null) {
      Date startTime = scheduleConfig.getStartTime();
      if (startTime != null) {
        String formattedTime = WorkflowConfig.getDefaultDateFormat().format(startTime);
        putSimpleConfig(WorkflowConfigProperty.StartTime.name(), formattedTime);
      }
      if (scheduleConfig.isRecurring()) {
        putSimpleConfig(WorkflowConfigProperty.RecurrenceUnit.name(),
            scheduleConfig.getRecurrenceUnit().toString());
        putSimpleConfig(WorkflowConfigProperty.RecurrenceInterval.name(),
            scheduleConfig.getRecurrenceInterval().toString());
      }
    }
    if (workflowType != null) {
      putSimpleConfig(WorkflowConfigProperty.WorkflowType.name(), workflowType);
    }

    if (jobTypes != null) {
      putMapConfig(WorkflowConfigProperty.JobTypes.name(), jobTypes);
    }
    putSimpleConfig(ResourceConfigProperty.MONITORING_DISABLED.toString(),
        String.valueOf(DEFAULT_MONITOR_DISABLE));
  }

  public String getWorkflowId() {
    String workflowId = getSimpleConfig(WorkflowConfigProperty.WorkflowID.name());
    return workflowId != null ? workflowId : getId();
  }

  public JobDag getJobDag() {
    if (_jobDag == null) {
      _jobDag = simpleConfigContains(WorkflowConfigProperty.Dag.name())
          ? JobDag.fromJson(getSimpleConfig(WorkflowConfigProperty.Dag.name()))
          : DEFAULT_JOB_DAG;
    }
    return _jobDag;
  }

  protected void setJobDag(JobDag jobDag) {
    try {
      putSimpleConfig(WorkflowConfigProperty.Dag.name(), jobDag.toJson());
    } catch (IOException ex) {
      throw new HelixException("Invalid job dag configuration!", ex);
    }
  }

  public int getParallelJobs() {
    return _record.getIntField(WorkflowConfigProperty.ParallelJobs.name(), DEFAULT_PARALLEL_JOBS);
  }

  public TargetState getTargetState() {
    return simpleConfigContains(WorkflowConfigProperty.TargetState.name())
        ? TargetState.valueOf(getSimpleConfig(WorkflowConfigProperty.TargetState.name()))
        : DEFAULT_TARGET_STATE;
  }

  public long getExpiry() {
    return _record.getLongField(WorkflowConfigProperty.Expiry.name(), DEFAULT_EXPIRY);
  }

  public long getJobPurgeInterval() {
    return _record.getLongField(WorkflowConfigProperty.JobPurgeInterval.name(),
        DEFAULT_JOB_PURGE_INTERVAL);
  }

  /**
   * This Failure threshold only works for generic workflow. Will be ignored by JobQueue
   * @return
   */
  public int getFailureThreshold() {
    return _record.getIntField(WorkflowConfigProperty.FailureThreshold.name(),
        DEFAULT_FAILURE_THRESHOLD);
  }

  /**
   * Determine the number of jobs that this workflow can accept before rejecting further jobs,
   * this field is only used when a workflow is not terminable.
   * @return queue capacity
   */
  public int getCapacity() {
    return _record.getIntField(WorkflowConfigProperty.capacity.name(), DEFAULT_CAPACITY);
  }

  public String getWorkflowType() {
    return simpleConfigContains(WorkflowConfigProperty.WorkflowType.name())
        ? getSimpleConfig(WorkflowConfigProperty.WorkflowType.name())
        : null;
  }

  public boolean isTerminable() {
    return _record.getBooleanField(WorkflowConfigProperty.Terminable.name(), DEFAULT_TERMINABLE);
  }

  public ScheduleConfig getScheduleConfig() {
    return parseScheduleFromConfigMap(getSimpleConfigs());
  }

  public boolean isRecurring() {
    return simpleConfigContains(WorkflowConfigProperty.StartTime.name())
        && simpleConfigContains(WorkflowConfigProperty.RecurrenceInterval.name())
        && simpleConfigContains(WorkflowConfigProperty.RecurrenceUnit.name());
  }

  public boolean isJobQueue() {
    return _record.getBooleanField(WorkflowConfigProperty.IsJobQueue.name(), DEFAULT_JOB_QUEUE)
        || !isTerminable();
  }

  protected void setJobTypes(Map<String, String> jobTypes) {
    putMapConfig(WorkflowConfigProperty.JobTypes.name(), jobTypes);
  }

  public Map<String, String> getJobTypes() {
    return mapConfigContains(WorkflowConfigProperty.JobTypes.name())
        ? getMapConfig(WorkflowConfigProperty.JobTypes.name())
        : null;
  }

  public boolean isAllowOverlapJobAssignment() {
    return _record.getBooleanField(WorkflowConfigProperty.AllowOverlapJobAssignment.name(),
        DEFAULT_ALLOW_OVERLAP_JOB_ASSIGNMENT);
  }

  public long getTimeout() {
    return _record.getLongField(WorkflowConfigProperty.Timeout.name(),
        TaskConstants.DEFAULT_NEVER_TIMEOUT);
  }

  public static SimpleDateFormat getDefaultDateFormat() {
    SimpleDateFormat defaultDateFormat = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");
    defaultDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

    return defaultDateFormat;
  }

  /**
   * Get the scheduled start time of the workflow.
   * @return start time if the workflow schedule is set, null if no schedule config set.
   */
  public Date getStartTime() {
    // Workflow with non-scheduled config is ready to schedule immediately.
    try {
      return simpleConfigContains(WorkflowConfigProperty.StartTime.name()) ? WorkflowConfig
          .getDefaultDateFormat().parse(getSimpleConfig(WorkflowConfigProperty.StartTime.name()))
          : null;
    } catch (ParseException e) {
      LOG.error("Unparseable date " + getSimpleConfig(WorkflowConfigProperty.StartTime.name()), e);
    }
    return null;
  }

  public Map<String, String> getResourceConfigMap() {
    return getSimpleConfigs();
  }

  /**
   * Get a ScheduleConfig from a workflow config string map
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
        LOG.error("Unparseable date " + cfg.get(WorkflowConfigProperty.StartTime.name()), e);
        return null;
      }
    }
    if (cfg.containsKey(WorkflowConfigProperty.RecurrenceUnit.name())
        && cfg.containsKey(WorkflowConfigProperty.RecurrenceInterval.name())) {
      return ScheduleConfig.recurringFromDate(startTime,
          TimeUnit.valueOf(cfg.get(WorkflowConfigProperty.RecurrenceUnit.name())),
          Long.parseLong(cfg.get(WorkflowConfigProperty.RecurrenceInterval.name())));
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
    return Builder.fromMap(configs).setWorkflowId(property.getId()).build();
  }

  public static class Builder {
    private String _workflowId = null;
    private JobDag _taskDag = DEFAULT_JOB_DAG;
    private int _parallelJobs = DEFAULT_PARALLEL_JOBS;
    private TargetState _targetState = DEFAULT_TARGET_STATE;
    private long _expiry = DEFAULT_EXPIRY;
    private int _failureThreshold = DEFAULT_FAILURE_THRESHOLD;
    private boolean _isTerminable = DEFAULT_TERMINABLE;
    private int _capacity = DEFAULT_CAPACITY;
    private ScheduleConfig _scheduleConfig;
    private String _workflowType;
    private boolean _isJobQueue = DEFAULT_JOB_QUEUE;
    private Map<String, String> _jobTypes;
    private long _jobPurgeInterval = DEFAULT_JOB_PURGE_INTERVAL;
    private boolean _allowOverlapJobAssignment = DEFAULT_ALLOW_OVERLAP_JOB_ASSIGNMENT;
    private long _timeout = TaskConstants.DEFAULT_NEVER_TIMEOUT;
    private boolean _enableCompression = TaskConstants.DEFAULT_TASK_ENABLE_COMPRESSION;

    public WorkflowConfig build() {
      validate();

      return new WorkflowConfig(_workflowId, _taskDag, _parallelJobs, _targetState, _expiry,
          _failureThreshold, _isTerminable, _scheduleConfig, _capacity, _workflowType, _isJobQueue,
          _jobTypes, _jobPurgeInterval, _allowOverlapJobAssignment, _timeout);
    }

    public Builder() {
    }

    public Builder(String workflowId) {
      _workflowId = workflowId;
    }

    public Builder(WorkflowConfig workflowConfig) {
      _workflowId = workflowConfig.getWorkflowId();
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
      _jobTypes = workflowConfig.getJobTypes();
      _jobPurgeInterval = workflowConfig.getJobPurgeInterval();
      _allowOverlapJobAssignment = workflowConfig.isAllowOverlapJobAssignment();
      _timeout = workflowConfig.getTimeout();
    }

    public Builder setWorkflowId(String v) {
      _workflowId = v;
      return this;
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

    /**
     * The expiry time for this workflow. Helix may clean up the workflow information after the
     * expiry time from the completion of the workflow.
     * @param v
     * @param unit
     * @return
     */
    public Builder setExpiry(long v, TimeUnit unit) {
      _expiry = unit.toMillis(v);
      return this;
    }

    /**
     * The expiry time for this workflow. Helix may clean up the workflow information after the
     * expiry time from the completion of the workflow.
     * @param v in milliseconds
     * @return
     */
    public Builder setExpiry(long v) {
      _expiry = v;
      return this;
    }

    /**
     * The time periodical Helix should clean up all completed jobs. This config applies only on
     * JobQueue.
     * @param t in milliseconds
     * @return
     */
    public Builder setJobPurgeInterval(long t) {
      _jobPurgeInterval = t;
      return this;
    }

    /**
     * The max allowed numbers of failed jobs before Helix should marks the workflow failure.
     * @param failureThreshold
     * @return
     */
    public Builder setFailureThreshold(int failureThreshold) {
      _failureThreshold = failureThreshold;
      return this;
    }

    /**
     * This method only applies for JobQueue, will be ignored in generic workflows
     * @param capacity The max number of jobs allowed in the queue
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

    protected Builder setTerminable(boolean isTerminable) {
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

    /**
     * ONLY generic workflow can be timeouted. JobQueue does not allow to be timeouted.
     * @param timeout The timeout period
     * @return
     */
    public Builder setTimeout(long timeout) {
      _timeout = timeout;
      return this;
    }

    /**
     * Set if allow multiple jobs in one workflow to be assigned on one instance.
     * If not set, default configuration is false.
     * @param allowOverlapJobAssignment true if allow overlap assignment
     * @return This builder
     */
    public Builder setAllowOverlapJobAssignment(boolean allowOverlapJobAssignment) {
      _allowOverlapJobAssignment = allowOverlapJobAssignment;
      return this;
    }

    public Builder setEnableCompression(boolean enableCompression) {
      _enableCompression = enableCompression;
      return this;
    }

    @Deprecated
    public static Builder fromMap(Map<String, String> cfg) {
      Builder builder = new Builder();
      return builder.setConfigMap(cfg);
    }

    // TODO: Add user customized simple field clone.
    // TODO: Add API to set map fields. This API only set simple fields
    public Builder setConfigMap(Map<String, String> cfg) {
      if (cfg.containsKey(WorkflowConfigProperty.WorkflowID.name())) {
        setWorkflowId(cfg.get(WorkflowConfigProperty.WorkflowID.name()));
      }

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

      if (cfg.containsKey(WorkflowConfigProperty.JobPurgeInterval.name())) {
        long jobPurgeInterval =
            Long.valueOf(cfg.get(WorkflowConfigProperty.JobPurgeInterval.name()));
        if (jobPurgeInterval > 0) {
          setJobPurgeInterval(jobPurgeInterval);
        }
      }

      if (cfg.containsKey(WorkflowConfigProperty.FailureThreshold.name())) {
        int threshold = Integer.valueOf(cfg.get(WorkflowConfigProperty.FailureThreshold.name()));
        if (threshold >= 0) {
          setFailureThreshold(threshold);
        }
      }

      if (cfg.containsKey(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD)) {
        setEnableCompression(
            Boolean.parseBoolean(cfg.get(ZNRecord.ENABLE_COMPRESSION_BOOLEAN_FIELD)));
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

      if (cfg.containsKey(WorkflowConfigProperty.AllowOverlapJobAssignment.name())) {
        setAllowOverlapJobAssignment(
            Boolean.parseBoolean(cfg.get(WorkflowConfigProperty.AllowOverlapJobAssignment.name())));
      }

      if (cfg.containsKey(WorkflowConfigProperty.Timeout.name())) {
        setTimeout(Long.parseLong(cfg.get(WorkflowConfigProperty.Timeout.name())));
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

    public long getTimeout() {
      return _timeout;
    }

    public static Builder from(WorkflowBean workflowBean) {
      WorkflowConfig.Builder b = new WorkflowConfig.Builder();
      if (workflowBean.schedule != null) {
        b.setScheduleConfig(ScheduleConfig.from(workflowBean.schedule));
      }
      b.setExpiry(workflowBean.expiry);
      b.setWorkFlowType(workflowBean.workflowType);
      b.setEnableCompression(workflowBean.enableCompression);
      return b;
    }

    private void validate() {
      _taskDag.validate();

      if (_expiry < 0) {
        throw new HelixException(String.format("%s has invalid value %s",
            WorkflowConfigProperty.Expiry.name(), _expiry));
      } else if (_scheduleConfig != null && !_scheduleConfig.isValid()) {
        throw new HelixException(
            "Scheduler configuration is invalid. The configuration must have a start time if it is "
                + "one-time, and it must have a positive interval magnitude if it is recurring");
      }
    }
  }
}
