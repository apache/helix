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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Implementation of the instance status bean
 */
public class InstanceMonitor implements InstanceMonitorMBean {
  private final String _clusterName;
  private final String _participantName;
  private List<String> _tags;
  private long _disabledPartitions;
  private boolean _isUp;
  private boolean _isEnabled;
  private long _totalMessageReceived;

  /**
   * Initialize the bean
   * @param clusterName the cluster to monitor
   * @param participantName the instance whose statistics this holds
   */
  public InstanceMonitor(String clusterName, String participantName) {
    _clusterName = clusterName;
    _participantName = participantName;
    _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    _disabledPartitions = 0L;
    _isUp = false;
    _isEnabled = false;
    _totalMessageReceived = 0;
  }

  @Override
  public String getSensorName() {
    return String.format("%s.%s.%s.%s", ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, _clusterName,
        serializedTags(), _participantName);
  }

  @Override
  public long getOnline() {
    return _isUp ? 1 : 0;
  }

  @Override
  public long getEnabled() {
    return _isEnabled ? 1 : 0;
  }

  @Override
  public long getTotalMessageReceived() {
    return _totalMessageReceived;
  }

  @Override
  public long getDisabledPartitions() {
    return _disabledPartitions;
  }

  /**
   * Get all the tags currently on this instance
   * @return list of tags
   */
  public List<String> getTags() {
    return _tags;
  }

  /**
   * Get the name of the monitored instance
   * @return instance name as a string
   */
  public String getInstanceName() {
    return _participantName;
  }

  private String serializedTags() {
    return Joiner.on('|').skipNulls().join(_tags).toString();
  }

  /**
   * Update the gauges for this instance
   * @param tags current tags
   * @param disabledPartitions current disabled partitions
   * @param isLive true if running, false otherwise
   * @param isEnabled true if enabled, false if disabled
   */
  public synchronized void updateInstance(Set<String> tags,
      Map<String, List<String>> disabledPartitions, List<String> oldDisabledPartitions,
      boolean isLive, boolean isEnabled) {
    if (tags == null || tags.isEmpty()) {
      _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    } else {
      _tags = Lists.newArrayList(tags);
      Collections.sort(_tags);
    }
    _disabledPartitions = 0L;
    if (disabledPartitions != null) {
      for (List<String> partitions : disabledPartitions.values()) {
        if (partitions != null) {
          _disabledPartitions += partitions.size();
        }
      }
    }
    // TODO : Get rid of this when old API removed.
    if (oldDisabledPartitions != null) {
      _disabledPartitions += oldDisabledPartitions.size();
    }
    _isUp = isLive;
    _isEnabled = isEnabled;
  }

  /**
   * Increase message received for this instance
   * @param messageReceived received message numbers
   */
  public synchronized void increaseMessageCount(long messageReceived) {
    _totalMessageReceived += messageReceived;
  }

}
