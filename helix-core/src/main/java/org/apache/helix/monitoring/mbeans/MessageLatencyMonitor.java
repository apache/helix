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

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import org.apache.helix.HelixException;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.ParticipantStatusMonitor;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.apache.log4j.Logger;

import javax.management.JMException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

public class MessageLatencyMonitor {
  private static final Logger logger = Logger.getLogger(MessageLatencyMonitor.class.getName());
  private static final String MBEAN_DESCRIPTION = "Helix Message Latency Monitor";
  public static String MONITOR_TYPE_KW = "MonitorType";

  private static final MetricRegistry _metricRegistry = new MetricRegistry();
  private String _participantName;
  private ObjectName _objectName;

  private DynamicMBeanProvider _dynamicMBeanProvider;

  private SimpleDynamicMetric<Long> _totalMessageCount =
      new SimpleDynamicMetric("TotalMessageCount", 0l);
  private SimpleDynamicMetric<Long> _totalMessageLatency =
      new SimpleDynamicMetric("TotalMessageLatency", 0l);
  private HistogramDynamicMetric _messageLatencyGauge =
      new HistogramDynamicMetric("MessageLatencyGauge",
          _metricRegistry.histogram(toString() + "MessageLatencyGauge"));

  public MessageLatencyMonitor(String participantName) {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_totalMessageCount);
    attributeList.add(_totalMessageLatency);
    attributeList.add(_messageLatencyGauge);

    _participantName = participantName;
    _dynamicMBeanProvider = new DynamicMBeanProvider(String
        .format("%s.%s.%s.%s", ParticipantStatusMonitor.PARTICIPANT_STATUS_KEY, participantName,
            MONITOR_TYPE_KW, MessageLatencyMonitor.class.getSimpleName()), MBEAN_DESCRIPTION,
        attributeList);
  }

  public void updateLatency(Message message) {
    long latency = System.currentTimeMillis() - message.getCreateTimeStamp();
    logger.info(String.format("The latency of message %s is %d ms", message.getMsgId(), latency));

    _totalMessageCount.updateValue(_totalMessageCount.getValue() + 1);
    _totalMessageLatency.updateValue(_totalMessageLatency.getValue() + latency);
    _messageLatencyGauge.updateValue(latency);
  }

  public void register(String domainName) throws JMException {
    if (_objectName != null) {
      throw new HelixException("Monitor has already been registed: " + _objectName.toString());
    }
    _objectName = MBeanRegistrar
        .register(_dynamicMBeanProvider, domainName, ParticipantStatusMonitor.PARTICIPANT_KEY,
            _participantName, MONITOR_TYPE_KW, MessageLatencyMonitor.class.getSimpleName());
  }

  public void unregister() {
    if (_objectName != null) {
      MBeanRegistrar.unregister(_objectName);
    }
    _metricRegistry.removeMatching(new MetricFilter() {
      @Override
      public boolean matches(String name, Metric metric) {
        return name.startsWith(toString());
      }
    });
  }
}
