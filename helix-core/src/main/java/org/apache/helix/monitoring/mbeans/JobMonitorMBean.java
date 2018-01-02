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

import org.apache.helix.monitoring.SensorNameProvider;

/**
 * Job monitor MBean for jobs, which are shared among jobs with the same type.
 */
public interface JobMonitorMBean extends SensorNameProvider {

  /**
   * Get number of the succeeded jobs
   * @return
   */
  public long getSuccessfulJobCount();

  /**
   * Get number of failed jobs
   * @return
   */
  public long getFailedJobCount();

  /**
   * Get number of the aborted jobs
   * @return
   */
  public long getAbortedJobCount();

  /**
   * Get number of existing jobs registered
   * @return
   */
  public long getExistingJobGauge();

  /**
   * Get numbers of queued jobs, which are not running jobs
   * @return
   */
  public long getQueuedJobGauge();

  /**
   * Get numbers of running jobs
   * @return
   */
  public long getRunningJobGauge();

  /**
   * Get maximum latency of jobs running time. It will be cleared every hour
   * @return
   */
  public long getMaximumJobLatencyGauge();

  /**
   * Get job latency counter.
   * @return
   */
  public long getJobLatencyCount();
}
