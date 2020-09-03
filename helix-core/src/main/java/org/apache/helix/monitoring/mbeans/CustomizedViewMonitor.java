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
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
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
  private static final String TYPE_KEY = "Type";
  private static final String CLUSTER_KEY = "Cluster";
  private static final String CUSTOMIZED_VIEW = "CustomizedView";

  public CustomizedViewMonitor(String clusterName) {
    _clusterName = clusterName;
    _sensorName = String
        .format("%s.%s.%s", MonitorDomainNames.AggregatedView.name(),
            CUSTOMIZED_VIEW, _clusterName);
    _updateToAggregationLatencyGauge =
        new HistogramDynamicMetric(UPDATE_TO_AGGREGATION_LATENCY_GAUGE, new Histogram(
            new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_updateToAggregationLatencyGauge);
    doRegister(attributeList, MBEAN_DESCRIPTION, getMBeanObjectName());
    return this;
  }

  private ObjectName getMBeanObjectName() throws MalformedObjectNameException {
    return new ObjectName(String
        .format("%s:%s=%s,%s=%s", MonitorDomainNames.AggregatedView.name(), TYPE_KEY,
            CUSTOMIZED_VIEW, CLUSTER_KEY, _clusterName));
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  public void recordUpdateToAggregationLatency(long latency) {
    _updateToAggregationLatencyGauge.updateValue(latency);
  }
}
