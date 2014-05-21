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

import org.apache.helix.HelixDefinedState;
import org.apache.helix.api.State;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.model.StateModelDefinition;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class PerInstanceResourceMonitor implements PerInstanceResourceMonitorMBean {
  public static class BeanName {
    private final String _instanceName;
    private final String _resourceName;

    public BeanName(String instanceName, String resourceName) {
      if (instanceName == null || resourceName == null) {
        throw new NullPointerException("Illegal beanName. instanceName: " + instanceName
            + ", resourceName: " + resourceName);
      }
      _instanceName = instanceName;
      _resourceName = resourceName;
    }

    public String instanceName() {
      return _instanceName;
    }

    public String resourceName() {
      return _resourceName;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof BeanName)) {
        return false;
      }

      BeanName that = (BeanName) obj;
      return _instanceName.equals(that._instanceName) && _resourceName.equals(that._resourceName);
    }

    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public String toString() {
      return String.format("%s=%s,%s=%s", ClusterStatusMonitor.INSTANCE_DN_KEY, _instanceName,
          ClusterStatusMonitor.RESOURCE_DN_KEY, _resourceName);
    }
  }

  private final String _clusterName;
  private List<String> _tags;
  private final String _participantName;
  private final String _resourceName;
  private long _partitions;

  public PerInstanceResourceMonitor(String clusterName, String participantName, String resourceName) {
    _clusterName = clusterName;
    _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    _participantName = participantName;
    _resourceName = resourceName;
    _partitions = 0;
  }

  @Override
  public String getSensorName() {
    return Joiner
        .on('.')
        .join(
            ImmutableList.of(ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, _clusterName,
                serializedTags(), _participantName, _resourceName)).toString();
  }

  private String serializedTags() {
    return Joiner.on('|').skipNulls().join(_tags).toString();
  }

  @Override
  public long getPartitionGauge() {
    return _partitions;
  }

  public String getInstanceName() {
    return _participantName;
  }

  public String getResourceName() {
    return _resourceName;
  }

  /**
   * Update per-instance resource bean
   * @param stateMap partition->state
   * @tags tags instance tags
   * @param stateModelDef
   */
  public synchronized void update(Map<PartitionId, State> stateMap, Set<String> tags,
      StateModelDefinition stateModelDef) {
    if (tags == null || tags.isEmpty()) {
      _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    } else {
      _tags = Lists.newArrayList(tags);
      Collections.sort(_tags);
    }

    int cnt = 0;
    for (State state : stateMap.values()) {
      // Skip DROPPED and initial state (e.g. OFFLINE)
      if (state.equals(HelixDefinedState.DROPPED)
          || state.equals(stateModelDef.getTypedInitialState())) {
        continue;
      }
      cnt++;
    }
    _partitions = cnt;
  }

}
