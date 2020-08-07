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
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import org.apache.helix.model.CustomizedView;
import org.apache.helix.model.Partition;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomizedViewMonitor extends DynamicMBeanProvider {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedViewMonitor.class);

  private static final String MBEAN_DESCRIPTION = "Helix Customized View Aggregation Monitor";
  private final String _clusterName;
  private final String _sensorName;
  private HistogramDynamicMetric _updateToAggregationLatencyGauge;
  public static final String UPDATE_TO_AGGREGATION_LATENCY_GAUGE =
      "UpdateToAggregationLatencyGauge";

  public CustomizedViewMonitor(String clusterName) {
    _clusterName = clusterName;
    _sensorName = String.format("%s.%s", MonitorDomainNames.AggregatedView.name(), _clusterName);
    _updateToAggregationLatencyGauge =
        new HistogramDynamicMetric(UPDATE_TO_AGGREGATION_LATENCY_GAUGE, new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_updateToAggregationLatencyGauge);
    doRegister(attributeList, MBEAN_DESCRIPTION, getMBeanName());
    return this;
  }

  private ObjectName getMBeanName() throws MalformedObjectNameException {
    return new ObjectName(String
        .format("%s:%s=%s", MonitorDomainNames.AggregatedView.name(), "Cluster", _clusterName));
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  void recordUpdateToAggregationLatency(long latency) {
    if (_updateToAggregationLatencyGauge != null) {
      _updateToAggregationLatencyGauge.updateValue(latency);
    }
  }

  /**
   * Find updated customized states and report the aggregation latency of each customized state
   * @param updatedCustomizedViews Customized views that have been updated, obtained from CustomizedStateOutput
   * @param curCustomizedViews Current customized view values from the CustomizedViewCache
   * @param updatedStartTimestamps All customized state START_TIME property values from CustomizedStateOutput
   * @param updateSuccess If the customized view update to ZK is successful or not
   * @param endTime The timestamp when the new customized view is updated to ZK
   */
  public void reportLatency(List<CustomizedView> updatedCustomizedViews,
      Map<String, CustomizedView> curCustomizedViews,
      Map<String, Map<Partition, Map<String, Long>>> updatedStartTimestamps,
      boolean[] updateSuccess, long endTime, String clusterName) {
    if (updatedCustomizedViews == null || curCustomizedViews == null
        || updatedStartTimestamps == null) {
      LOG.warn("Cannot find updated time stamps for customized states, input parameter is null.");
      return;
    }

    List<Long> collectedTimestamps = new ArrayList<>();

    for (int i = 0; i < updatedCustomizedViews.size(); i++) {
      CustomizedView newCV = updatedCustomizedViews.get(i);
      String resourceName = newCV.getResourceName();

      if (!updateSuccess[i]) {
        LOG.warn("Customized views are not updated successfully for resource {} on cluster {}",
            resourceName, clusterName);
        continue;
      }

      CustomizedView oldCV =
          curCustomizedViews.getOrDefault(resourceName, new CustomizedView(resourceName));

      Map<String, Map<String, String>> newPartitionStateMaps = newCV.getRecord().getMapFields();
      Map<String, Map<String, String>> oldPartitionStateMaps = oldCV.getRecord().getMapFields();
      Map<Partition, Map<String, Long>> partitionStartTimeMaps =
          updatedStartTimestamps.getOrDefault(resourceName, Collections.emptyMap());

      for (Map.Entry<String, Map<String, String>> partitionStateMapEntry : newPartitionStateMaps
          .entrySet()) {
        String partitionName = partitionStateMapEntry.getKey();
        Map<String, String> newStateMap = partitionStateMapEntry.getValue();
        Map<String, String> oldStateMap =
            oldPartitionStateMaps.getOrDefault(partitionName, Collections.emptyMap());
        if (!newStateMap.equals(oldStateMap)) {
          Map<String, Long> partitionStartTimeMap = partitionStartTimeMaps
              .getOrDefault(new Partition(partitionName), Collections.emptyMap());

          for (Map.Entry<String, String> stateMapEntry : newStateMap.entrySet()) {
            String instanceName = stateMapEntry.getKey();
            if (!stateMapEntry.getValue().equals(oldStateMap.get(instanceName))) {
              long timestamp = partitionStartTimeMap.get(instanceName);
              if (timestamp > 0) {
                collectedTimestamps.add(timestamp);
              } else {
                LOG.warn(
                    "Failed to find customized state update time stamp for resource {} partition {}, instance {}, on cluster {} the number should be positive.",
                    resourceName, partitionName, instanceName, clusterName);
              }
            }
          }
        }
      }
    }
    collectedTimestamps.forEach(startTime -> recordUpdateToAggregationLatency(endTime - startTime));
  }
}
