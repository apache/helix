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

import org.apache.helix.HelixException;

import javax.management.MBeanAttributeInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * The abstract class for dynamic metrics that is used to emitting monitor data to DynamicMBean.
 *
 * @param <O> the type of metric object to initialize DynamicMetric
 * @param <T> the type of monitor data
 *            Note that monitor data type can be different from the metric object type.
 */
public abstract class DynamicMetric<O, T> {
  protected static final String DEFAULT_ATTRIBUTE_DESCRIPTION = "Attribute exposed for management";

  private final Set<MBeanAttributeInfo> _attributeInfoSet;
  private O _metricObject;

  /**
   * Instantiates a new Dynamic metric.
   *
   * @param metricName   the metric name
   * @param metricObject the metric object
   */
  public DynamicMetric(String metricName, O metricObject) {
    if (metricName == null || metricObject == null) {
      throw new HelixException("Failed to construct metric due to missing argument.");
    }
    _metricObject = metricObject;
    _attributeInfoSet =
        Collections.unmodifiableSet(generateAttributeInfos(metricName, metricObject));
  }

  /**
   * @return the attribute infos that are exposed in the DynamicMBean
   */
  public Collection<MBeanAttributeInfo> getAttributeInfos() {
    return _attributeInfoSet;
  }

  /**
   * @param attributeName the attribute name
   * @return the attribute value to update DynamicMBean
   */
  public abstract Object getAttributeValue(String attributeName);

  /**
   * @param newValue new value to update the DynamicMetric
   */
  public abstract void updateValue(T newValue);

  /**
   * Generate attribute infos set based on the input parameters
   * Override this method if the monitor data will be exposed into different attributes
   *
   * @param metricName   the metric name
   * @param metricObject the metric object
   * @return the MBeanAttributeInfo set
   */
  protected Set<MBeanAttributeInfo> generateAttributeInfos(String metricName, O metricObject) {
    Set<MBeanAttributeInfo> attributeInfoSet = new HashSet<>();
    attributeInfoSet.add(new MBeanAttributeInfo(metricName, metricObject.getClass().getName(),
        DEFAULT_ATTRIBUTE_DESCRIPTION, true, false, false));
    return attributeInfoSet;
  }

  /**
   * Gets metric object.
   *
   * @return the metric object
   */
  protected O getMetricObject() {
    return _metricObject;
  }

  /**
   * Sets metric object.
   *
   * @param metricObject the metric object
   */
  protected void setMetricObject(O metricObject) {
    _metricObject = metricObject;
  }
}

