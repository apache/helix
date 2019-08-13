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
import java.util.concurrent.TimeUnit;
import org.apache.helix.model.Message;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;

import javax.management.JMException;
import java.util.ArrayList;
import java.util.List;

public class MessageLatencyMonitor extends DynamicMBeanProvider {
  private static final String MBEAN_DESCRIPTION = "Helix Message Latency Monitor";
  private final String _sensorName;
  private final String _domainName;
  private final String _participantName;

  private SimpleDynamicMetric<Long> _totalMessageCount;
  private SimpleDynamicMetric<Long> _totalMessageLatency;
  private HistogramDynamicMetric _messageLatencyGauge;

  public MessageLatencyMonitor(String domainName, String participantName) throws JMException {
    _domainName = domainName;
    _participantName = participantName;
    _sensorName = String.format("%s.%s", ParticipantMessageMonitor.PARTICIPANT_STATUS_KEY,
        "MessageLatency");

    _messageLatencyGauge = new HistogramDynamicMetric("MessagelatencyGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(getResetIntervalInMs(), TimeUnit.MILLISECONDS)));
    _totalMessageLatency = new SimpleDynamicMetric("TotalMessageLatency", 0l);
    _totalMessageCount = new SimpleDynamicMetric("TotalMessageCount", 0l);
  }

  @Override
  public String getSensorName() {
    return _sensorName;
  }

  public void updateLatency(Message message) {
    long latency = System.currentTimeMillis() - message.getCreateTimeStamp();
    _logger.info(String.format("The latency of message %s is %d ms", message.getMsgId(), latency));

    _totalMessageCount.updateValue(_totalMessageCount.getValue() + 1);
    _totalMessageLatency.updateValue(_totalMessageLatency.getValue() + latency);
    _messageLatencyGauge.updateValue(latency);
  }

  @Override
  public MessageLatencyMonitor register() throws JMException {
    List<DynamicMetric<?, ?>> attributeList = new ArrayList<>();
    attributeList.add(_totalMessageCount);
    attributeList.add(_totalMessageLatency);
    attributeList.add(_messageLatencyGauge);
    doRegister(attributeList, MBEAN_DESCRIPTION, _domainName, ParticipantMessageMonitor.PARTICIPANT_KEY,
        _participantName, "MonitorType", MessageLatencyMonitor.class.getSimpleName());

    return this;
  }
}
