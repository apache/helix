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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.management.JMException;
import javax.management.ObjectName;
import org.apache.helix.monitoring.StateTransitionContext;
import org.apache.helix.monitoring.StateTransitionDataPoint;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMBeanProvider;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.DynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.HistogramDynamicMetric;
import org.apache.helix.monitoring.mbeans.dynamicMBeans.SimpleDynamicMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO convert StateTransitionStatMonitor to extends DynamicMBeanProvider.
// Note this might change the attributes name.
public class StateTransitionStatMonitor extends DynamicMBeanProvider {
  private static final Logger _logger = LoggerFactory.getLogger(StateTransitionStatMonitor.class);
  private List<DynamicMetric<?, ?>> _attributeList;
  // For registering dynamic metrics
  private final ObjectName _initObjectName;

  private SimpleDynamicMetric<Long> _totalStateTransitionCounter;
  private SimpleDynamicMetric<Long> _totalFailedTransitionCounter;
  private SimpleDynamicMetric<Long> _totalSuccessTransitionCounter;

  private HistogramDynamicMetric _transitionLatencyGauge;
  private HistogramDynamicMetric _transitionExecutionLatencyGauge;
  private HistogramDynamicMetric _transitionMessageLatency;

  StateTransitionContext _context;

  public StateTransitionStatMonitor(StateTransitionContext context, ObjectName objectName) {
    _context = context;
    _initObjectName = objectName;
    _attributeList = new ArrayList<>();
    _totalStateTransitionCounter = new SimpleDynamicMetric<>("TotalStateTransitionCounter", 0L);
    _totalFailedTransitionCounter = new SimpleDynamicMetric<>("TotalFailedTransitionCounter", 0L);
    _totalSuccessTransitionCounter = new SimpleDynamicMetric<>("TotalSuccessTransitionCounter", 0L);

    _transitionLatencyGauge = new HistogramDynamicMetric("TransitionLatencyGauge", new Histogram(
        new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
    _transitionExecutionLatencyGauge = new HistogramDynamicMetric("TransitionExecutionLatencyGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
    _transitionMessageLatency = new HistogramDynamicMetric("TransitionMessageLatencyGauge",
        new Histogram(
            new SlidingTimeWindowArrayReservoir(DEFAULT_RESET_INTERVAL_MS, TimeUnit.MILLISECONDS)));
  }

  @Override
  public DynamicMBeanProvider register() throws JMException {
    _attributeList.add(_totalStateTransitionCounter);
    _attributeList.add(_totalFailedTransitionCounter);
    _attributeList.add(_totalSuccessTransitionCounter);
    _attributeList.add(_transitionLatencyGauge);
    _attributeList.add(_transitionExecutionLatencyGauge);
    _attributeList.add(_transitionMessageLatency);
    doRegister(_attributeList, _initObjectName);
    return this;
  }

  public StateTransitionContext getContext() {
    return _context;
  }

  public String getSensorName() {
    return String.format("StateTransitionStat.%s.%s.%s", _context.getClusterName(),
        _context.getResourceName(), _context.getTransition());
  }

  public void addDataPoint(StateTransitionDataPoint data) {
    incrementSimpleDynamicMetric(_totalStateTransitionCounter);
    if (data.getSuccess()) {
      incrementSimpleDynamicMetric(_totalSuccessTransitionCounter);
    } else {
      incrementSimpleDynamicMetric(_totalFailedTransitionCounter);
    }

    _transitionLatencyGauge.updateValue(data.getTotalDelay());
    _transitionExecutionLatencyGauge.updateValue(data.getExecutionDelay());
    _transitionMessageLatency.updateValue(data.getMessageLatency());
  }
}
