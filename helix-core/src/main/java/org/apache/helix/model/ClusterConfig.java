package org.apache.helix.model;

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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;

/**
 * Cluster configurations
 */
public class ClusterConfig extends HelixProperty {
  /**
   * Configurable characteristics of a cluster
   */
  public enum ClusterConfigProperty {
    HELIX_DISABLE_PIPELINE_TRIGGERS,
    TOPOLOGY,  // cluster topology definition, for example, "/zone/rack/host/instance"
    PERSIST_BEST_POSSIBLE_ASSIGNMENT,
    FAULT_ZONE_TYPE, // the type in which isolation should be applied on when Helix places the replicas from same partition.
    DELAY_REBALANCE_DISABLED,  // enabled the delayed rebalaning in case node goes offline.
    DELAY_REBALANCE_TIME,     // delayed time in ms that the delay time Helix should hold until rebalancing.
    BATCH_STATE_TRANSITION_MAX_THREADS,
    MAX_CONCURRENT_TASK_PER_INSTANCE
  }
  private final static int DEFAULT_MAX_CONCURRENT_TASK_PER_INSTANCE = 40;

  /**
   * Instantiate for a specific cluster
   *
   * @param cluster the cluster identifier
   */
  public ClusterConfig(String cluster) {
    super(cluster);
  }

  /**
   * Instantiate with a pre-populated record
   *
   * @param record a ZNRecord corresponding to a cluster configuration
   */
  public ClusterConfig(ZNRecord record) {
    super(record);
  }

  /**
   * Whether to persist best possible assignment in a resource's idealstate.
   *
   * @return
   */
  public Boolean isPersistBestPossibleAssignment() {
    return _record
        .getBooleanField(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString(), false);
  }

  /**
   * Enable/Disable persist best possible assignment in a resource's idealstate.
   *
   * @return
   */
  public void setPersistBestPossibleAssignment(Boolean enable) {
    if (enable == null) {
      _record.getSimpleFields()
          .remove(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString());
    } else {
      _record.setBooleanField(ClusterConfigProperty.PERSIST_BEST_POSSIBLE_ASSIGNMENT.toString(),
          enable);
    }
  }

  /**
   *
   * @return
   */
  public Boolean isPipelineTriggersDisabled() {
    return _record
        .getBooleanField(ClusterConfigProperty.HELIX_DISABLE_PIPELINE_TRIGGERS.toString(), false);
  }

  public long getRebalanceDelayTime() {
    return _record.getLongField(ClusterConfigProperty.DELAY_REBALANCE_TIME.name(), -1);
  }

  public boolean isDelayRebalaceDisabled() {
    return _record.getBooleanField(ClusterConfigProperty.DELAY_REBALANCE_DISABLED.name(), false);
  }

  /**
   * Set the customized batch message thread pool size
   *
   * @return
   */
  public void setBatchStateTransitionMaxThreads(int maxThreads) {
    _record
        .setIntField(ClusterConfigProperty.BATCH_STATE_TRANSITION_MAX_THREADS.name(), maxThreads);
  }

  /**
   * Get the customized batch message thread pool size
   *
   * @return
   */
  public int getBatchStateTransitionMaxThreads() {
    return _record.getIntField(ClusterConfigProperty.BATCH_STATE_TRANSITION_MAX_THREADS.name(), -1);
  }

  /**
   * Get maximum allowed running task count on all instances in this cluster.
   * Instance level configuration will override cluster configuration.
   * @return the maximum task count
   */
  public int getMaxConcurrentTaskPerInstance() {
    return _record.getIntField(ClusterConfigProperty.MAX_CONCURRENT_TASK_PER_INSTANCE.name(),
        DEFAULT_MAX_CONCURRENT_TASK_PER_INSTANCE);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ClusterConfig) {
      ClusterConfig that = (ClusterConfig) obj;

      if (this.getId().equals(that.getId())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  /**
   * Get the name of this resource
   *
   * @return the instance name
   */
  public String getClusterName() {
    return _record.getId();
  }
}

