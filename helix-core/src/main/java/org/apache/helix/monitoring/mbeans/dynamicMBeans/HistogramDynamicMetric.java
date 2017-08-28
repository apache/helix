package org.apache.helix.monitoring.mbeans.dynamicMBeans;

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
import com.codahale.metrics.Snapshot;
import org.apache.log4j.Logger;

import javax.management.MBeanAttributeInfo;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * The dynamic metric that accept Long monitor data and emits histogram information based on the input
 */
public class HistogramDynamicMetric extends DynamicMetric<Histogram, Long> {
  private static final Logger _logger = Logger.getLogger(HistogramDynamicMetric.class);

  private final Set<String> _attributeNameSet;

  /**
   * The enum statistic attributes
   */
  enum SnapshotAttribute {
    Pct75th("get75thPercentile", "75Pct"),
    Pct95th("get95thPercentile", "95Pct"),
    Pct99th("get99thPercentile", "99Pct"),
    Max("getMax", "Max"),
    Mean("getMean", "Mean"),
    StdDev("getStdDev", "StdDev");

    final String _getMethodName;
    final String _attributeName;

    SnapshotAttribute(String getMethodName, String attributeName) {
      _getMethodName = getMethodName;
      _attributeName = attributeName;
    }
  }

  /**
   * Instantiates a new Histogram dynamic metric.
   *
   * @param metricName   the metric name
   * @param metricObject the metric object
   */
  public HistogramDynamicMetric(String metricName, Histogram metricObject) {
    super(metricName, metricObject);

    _attributeNameSet = new HashSet<>();
    Iterator<MBeanAttributeInfo> iter = getAttributeInfos().iterator();
    while (iter.hasNext()) {
      MBeanAttributeInfo attributeInfo = iter.next();
      _attributeNameSet.add(attributeInfo.getName());
    }
  }

  @Override
  public Number getAttributeValue(String attributeName) {
    if (!_attributeNameSet.contains(attributeName)) {
      return null;
    }

    String[] attributeNameParts = attributeName.split("\\.");
    if (attributeNameParts.length == 2) {
      try {
        SnapshotAttribute snapshotAttribute = SnapshotAttribute.valueOf(attributeNameParts[1]);
        Method getMethod = Snapshot.class.getMethod(snapshotAttribute._getMethodName);
        Snapshot snapshot = getMetricObject().getSnapshot();
        if (snapshot != null) {
          return (Number) getMethod.invoke(snapshot);
        }
      } catch (Exception ex) {
        _logger
            .error(String.format("Failed to get Snapshot value for attribute: %s", attributeName),
                ex);
      }
    } else {
      _logger.error(String.format("Invalid attribute name format: %s", attributeName));
    }
    return null;
  }

  @Override
  public void updateValue(Long value) {
    getMetricObject().update(value);
  }

  @Override
  protected Set<MBeanAttributeInfo> generateAttributeInfos(String metricName,
      Histogram metricObject) {
    Set<MBeanAttributeInfo> attributeInfoSet = new HashSet<>();

    for (SnapshotAttribute snapshotAttribute : SnapshotAttribute.values()) {
      try {
        Method getMethod = Snapshot.class.getMethod(snapshotAttribute._getMethodName);
        attributeInfoSet.add(
            new MBeanAttributeInfo(getSnapshotAttributeName(metricName, snapshotAttribute.name()),
                getMethod.getReturnType().getName(), DEFAULT_ATTRIBUTE_DESCRIPTION, true, false,
                false));
      } catch (NoSuchMethodException e) {
        _logger.error(
            "Cannot generate AttributeInfo for Attribute: " + snapshotAttribute._attributeName, e);
      }
    }

    return attributeInfoSet;
  }

  private String getSnapshotAttributeName(String metricName, String snapshotAttribute) {
    return String.format("%s.%s", metricName, snapshotAttribute);
  }
}
