package org.apache.helix.monitoring.mbeans;

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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.helix.task.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobMonitor extends DynamicMBeanProvider {

  private static final String JOB_KEY = "Job";
  private static final Logger LOG = LoggerFactory.getLogger(JobMonitor.class);
  private static final long DEFAULT_RESET_INTERVAL_MS = 60 * 60 * 1000; // 1 hour

  // For registering dynamic metrics
  private final ObjectName _initObjectName;

  private String _clusterName;
  private String _jobType;
  private long _lastResetTime;

  // Counters
  private SimpleDynamicMetric<Long> _successfulJobCount;
  private SimpleDynamicMetric<Long> _failedJobCount;
  private SimpleDynamicMetric<Long> _abortedJobCount;

  // Gauges
  private SimpleDynamicMetric<Long> _existingJobGauge;
  private SimpleDynamicMetric<Long> _queuedJobGauge;
  private SimpleDynamicMetric<Long> _runningJobGauge;
  @Deprecated // To be removed (replaced by jobLatencyGauge Histogram)
  private SimpleDynamicMetric<Long> _maximumJobLatencyGauge;
  @Deprecated // To be removed (replaced by jobLatencyGauge Histogram)
  private SimpleDynamicMetric<Long> _jobLatencyCount;

  // Histogram
  private HistogramDynamicMetric _jobLatencyGauge;
  private HistogramDynamicMetric _submissionToProcessDelayGauge;
  private HistogramDynamicMetric _submissionToScheduleDelayGauge;
  private HistogramDynamicMetric _controllerInducedDelayGauge;

  public JobMonitor(String clusterName, String jobType, ObjectName objectName) {
    _clusterName = clusterName;
    _jobType = jobType;
    _initObjectName = objectName;
    _lastResetTime = System.currentTimeMillis();

    // Instantiate simple dynamic metrics
    _successfulJobCount = new SimpleDynamicMetric("SuccessfulJobCount", 0L);
    _failedJobCount = new SimpleDynamicMetric("FailedJobCount", 0L);
    _abortedJobCount = new SimpleDynamicMetric("AbortedJobCount", 0L);
    _existingJobGauge = new SimpleDynamicMetric("ExistingJobGauge", 0L);
    _queuedJobGauge = new SimpleDynamicMetric("QueuedJobGauge", 0L);
    _runningJobGauge = new SimpleDynamicMetric("RunningJobGauge", 0L);
    _maximumJobLatencyGauge = new SimpleDynamicMetric("MaximumJobLatencyGauge", 0L);
    _jobLatencyCount = new SimpleDynamicMetric("JobLatencyCount", 0L);

    // Instantiate histogram dynamic metrics
    _jobLatencyGauge = new HistogramDynamicMetric("JobLatencyGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _submissionToProcessDelayGauge = new HistogramDynamicMetric("SubmissionToProcessDelayGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _submissionToScheduleDelayGauge = new HistogramDynamicMetric("SubmissionToScheduleDelayGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _controllerInducedDelayGauge = new HistogramDynamicMetric("ControllerInducedDelayGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s", _clusterName, JOB_KEY, _jobType);
  }

  public String getJobType() {
    return _jobType;
  }

  /**
   * Update job counters with transition state
   * @param to The to state of job, cleaned by ZK when it is null
   */
  public void updateJobMetricsWithLatency(TaskState to) {
    updateJobMetricsWithLatency(to, 0);
  }

  public void updateJobMetricsWithLatency(TaskState to, long latency) {
    // TODO maybe use separate TIMED_OUT counter later
    if (to.equals(TaskState.FAILED) || to.equals(TaskState.TIMED_OUT)) {
      incrementSimpleDynamicMetric(_failedJobCount);
    } else if (to.equals(TaskState.COMPLETED)) {
      incrementSimpleDynamicMetric(_successfulJobCount);
      // Only count succeeded jobs
      _maximumJobLatencyGauge.updateValue(Math.max(_maximumJobLatencyGauge.getValue(), latency));
      if (latency > 0) {
        incrementSimpleDynamicMetric(_jobLatencyCount, latency);
        _jobLatencyGauge.updateValue(latency);
      }
    } else if (to.equals(TaskState.ABORTED)) {
      incrementSimpleDynamicMetric(_abortedJobCount);
    }
  }

  /**
   * Reset job gauges
   */
  public void resetJobGauge() {
    _queuedJobGauge.updateValue(0L);
    _existingJobGauge.updateValue(0L);
    _runningJobGauge.updateValue(0L);
    if (_lastResetTime + DEFAULT_RESET_INTERVAL_MS < System.currentTimeMillis()) {
      _lastResetTime = System.currentTimeMillis();
      _maximumJobLatencyGauge.updateValue(0L);
    }
  }

  /**
   * Refresh job gauges
   * @param to The current state of job
   */
  public void updateJobGauge(TaskState to) {
    incrementSimpleDynamicMetric(_existingJobGauge);
    if (to == null || to.equals(TaskState.NOT_STARTED)) {
      incrementSimpleDynamicMetric(_queuedJobGauge);
    } else if (to.equals(TaskState.IN_PROGRESS)) {
      incrementSimpleDynamicMetric(_runningJobGauge);
    }
  }

  /**
   * Update SubmissionToProcessDelay to its corresponding HistogramDynamicMetric.
   * @param delay
   */
  public void updateSubmissionToProcessDelayGauge(long delay) {
    _submissionToProcessDelayGauge.updateValue(delay);
  }

  /**
   * Update SubmissionToScheduleDelay to its corresponding HistogramDynamicMetric.
   * @param delay
   */
  public void updateSubmissionToScheduleDelayGauge(long delay) {
    _submissionToScheduleDelayGauge.updateValue(delay);
  }

  /**
   * Update ControllerInducedDelay to its corresponding HistogramDynamicMetric.
   * @param delay
   */
  public void updateControllerInducedDelayGauge(long delay) {
    _controllerInducedDelayGauge.updateValue(delay);
  }

  /**
   * This method registers the dynamic metrics.
   * @return
   * @throws JMException
   */
  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_successfulJobCount);
    attributeList.add(_failedJobCount);
    attributeList.add(_abortedJobCount);
    attributeList.add(_existingJobGauge);
    attributeList.add(_queuedJobGauge);
    attributeList.add(_runningJobGauge);
    attributeList.add(_maximumJobLatencyGauge);
    attributeList.add(_jobLatencyCount);
    attributeList.add(_jobLatencyGauge);
    attributeList.add(_submissionToProcessDelayGauge);
    attributeList.add(_submissionToScheduleDelayGauge);
    attributeList.add(_controllerInducedDelayGauge);
    doRegister(attributeList, _initObjectName);
    return this;
  }
}
