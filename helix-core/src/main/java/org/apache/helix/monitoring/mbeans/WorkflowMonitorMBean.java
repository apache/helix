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
 * Workflow monitor MBean for workflows, which are shared among workflows with the same type.
 */
public interface WorkflowMonitorMBean extends SensorNameProvider {

  /**
   * Get number of succeeded workflows
   * @return
   */
  public long getSuccessfulWorkflowCount();

  /**
   * Get number of failed workflows
   * @return
   */
  public long getFailedWorkflowCount();

  /**
   * Get number of current failed workflows
   */
  public long getFailedWorkflowGauge();

  /**
   * Get number of current existing workflows
   * @return
   */
  public long getExistingWorkflowGauge();

  /**
   * Get number of queued but not started workflows
   * @return
   */
  public long getQueuedWorkflowGauge();

  /**
   * Get number of running workflows
   * @return
   */
  public long getRunningWorkflowGauge();

  /**
   * Get workflow latency count
   * @return
   */
  public long getWorkflowLatencyCount();

  /**
   * Get maximum workflow latency gauge. It will be reset in 1 hour.
   * @return
   */
  public long getMaximumWorkflowLatencyGauge();
}
