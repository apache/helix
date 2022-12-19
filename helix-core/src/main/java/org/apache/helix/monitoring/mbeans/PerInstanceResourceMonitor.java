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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.helix.HelixDefinedState;
import org.apache.helix.model.Partition;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerInstanceResourceMonitor extends DynamicMBeanProvider {
  private static final Logger LOG = LoggerFactory.getLogger(PerInstanceResourceMonitor.class);
  private static final String MBEAN_DESCRIPTION = "Per Instance Resource Monitor";

  public static class BeanName {
    private final String _instanceName;
    private final String _resourceName;
    private final String _clusterName;

    public BeanName(String clusterName, String instanceName, String resourceName) {
      if (clusterName == null || instanceName == null || resourceName == null) {
        throw new NullPointerException(
            "Illegal beanName. clusterName: " + clusterName + ", instanceName: " + instanceName
                + ", resourceName: " + resourceName);
      }
      _clusterName = clusterName;
      _instanceName = instanceName;
      _resourceName = resourceName;
    }

    public String instanceName() {
      return _instanceName;
    }

    public String resourceName() {
      return _resourceName;
    }

    public ObjectName objectName() {
      try {
        return new ObjectName(MonitorDomainNames.ClusterStatus.name() + ":" + this);
      } catch (MalformedObjectNameException e) {
        LOG.error("Failed to create object name for cluster: {}, instance: {}, resource: {}.",
            _clusterName, _instanceName, _resourceName);
      }
      return null;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BeanName)) {
        return false;
      }

      BeanName that = (BeanName) obj;
      return _clusterName.equals(that._clusterName) && _instanceName.equals(that._instanceName)
          && _resourceName.equals(that._resourceName);
    }

    @Override
    public int hashCode() {
      return 31 * 31 * _clusterName.hashCode()
          + 31 * _instanceName.hashCode()
          + _resourceName.hashCode();
    }

    @Override
    public String toString() {
      return ClusterStatusMonitor.CLUSTER_DN_KEY + "=" + _clusterName + ","
          + ClusterStatusMonitor.INSTANCE_DN_KEY + "=" + _instanceName + ","
          + ClusterStatusMonitor.RESOURCE_DN_KEY + "=" + _resourceName;
    }
  }

  private final String _clusterName;
  private List<String> _tags;
  private final String _participantName;
  private final String _resourceName;
  private SimpleDynamicMetric<Long> _partitions;

  public PerInstanceResourceMonitor(String clusterName, String participantName,
      String resourceName) {
    _clusterName = clusterName;
    _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    _participantName = participantName;
    _resourceName = resourceName;
    _partitions = new SimpleDynamicMetric<>("PartitionGauge", 0L);
  }

  @Override
  public String getSensorName() {
    return Joiner.on('.').join(ImmutableList
        .of(ClusterStatusMonitor.PARTICIPANT_STATUS_KEY, _clusterName, serializedTags(),
            _participantName, _resourceName));
  }

  private String serializedTags() {
    return Joiner.on('|').skipNulls().join(_tags);
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
  public synchronized void update(Map<Partition, String> stateMap, Set<String> tags,
      StateModelDefinition stateModelDef) {
    if (tags == null || tags.isEmpty()) {
      _tags = ImmutableList.of(ClusterStatusMonitor.DEFAULT_TAG);
    } else {
      _tags = Lists.newArrayList(tags);
      Collections.sort(_tags);
    }

    int cnt = 0;
    for (String state : stateMap.values()) {
      // Skip DROPPED and initial state (e.g. OFFLINE)
      if (state.equalsIgnoreCase(HelixDefinedState.DROPPED.name()) || state
          .equalsIgnoreCase(stateModelDef.getInitialState())) {
        continue;
      }
      cnt++;
    }
    _partitions.updateValue((long) cnt);
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_partitions);
    doRegister(attributeList, MBEAN_DESCRIPTION,
        new BeanName(_clusterName, _participantName, _resourceName).objectName());
    return this;
  }
}
