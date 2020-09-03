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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;

import org.apache.helix.monitoring.SensorNameProvider;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dynamic MBean provider that reporting DynamicMetric attributes
 */
public abstract class DynamicMBeanProvider implements DynamicMBean, SensorNameProvider {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());
  protected static final long DEFAULT_RESET_INTERVAL_MS = 60 * 1000; // Reset time every minute
  private static final String HELIX_MONITOR_TIME_WINDOW_LENGTH_MS =
      "helix.monitor.slidingTimeWindow.ms";
  private static final String SENSOR_NAME_TAG = "SensorName";
  private static final String DEFAULT_DESCRIPTION =
      "Information on the management interface of the MBean";

  // Attribute name to the DynamicMetric object mapping
  private Map<String, DynamicMetric> _attributeMap = new HashMap<>();
  private ObjectName _objectName = null;
  private MBeanInfo _mBeanInfo;

  /**
   * Instantiates a new Dynamic MBean provider.
   * @param dynamicMetrics Dynamic Metrics that are exposed by this provider
   * @param description the MBean description
   * @param domain the MBean domain name
   * @param keyValuePairs the MBean object name components
   */
  protected synchronized boolean doRegister(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      String description, String domain, String... keyValuePairs) throws JMException {
    return doRegister(dynamicMetrics, description,
        MBeanRegistrar.buildObjectName(domain, keyValuePairs));
  }

  /**
   * Instantiates a new Dynamic MBean provider.
   * @param dynamicMetrics Dynamic Metrics that are exposed by this provider
   * @param description the MBean description
   * @param objectName the proposed MBean ObjectName
   */
  protected synchronized boolean doRegister(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      String description, ObjectName objectName) throws JMException {
    if (_objectName != null) {
      _logger.debug("Mbean {} has already been registered. Ignore register request.",
          objectName.getCanonicalName());
      return false;
    }
    updateAttributesInfo(dynamicMetrics, description);
    _objectName = MBeanRegistrar.register(this, objectName);
    return true;
  }

  protected synchronized boolean doRegister(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      ObjectName objectName) throws JMException {
    return doRegister(dynamicMetrics, null, objectName);
  }

  /**
   * Updates the Dynamic MBean provider with new metric list.
   * If the pass-in metrics collection is empty, the original attributes will be removed.
   *
   * @param description description of the MBean
   * @param dynamicMetrics the DynamicMetrics. Empty collection will remove the metric attributes.
   */
  protected void updateAttributesInfo(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      String description) {
    if (dynamicMetrics == null) {
      _logger.warn("Cannot update attributes info because dynamicMetrics is null.");
      return;
    }

    List<MBeanAttributeInfo> attributeInfoList = new ArrayList<>();
    // Use a new attribute map to avoid concurrency issue.
    Map<String, DynamicMetric> newAttributeMap = new HashMap<>();

    // Get all attributes that can be emitted by the dynamicMetrics.
    for (DynamicMetric<?, ?> dynamicMetric : dynamicMetrics) {
      for (MBeanAttributeInfo attributeInfo : dynamicMetric.getAttributeInfos()) {
        // Info list to create MBean info
        attributeInfoList.add(attributeInfo);
        // Attribute mapping for getting attribute value when getAttribute() is called
        newAttributeMap.put(attributeInfo.getName(), dynamicMetric);
      }
    }

    // SensorName
    attributeInfoList.add(new MBeanAttributeInfo(SENSOR_NAME_TAG, String.class.getName(),
        "The name of the metric sensor", true, false, false));

    MBeanConstructorInfo constructorInfo = new MBeanConstructorInfo(
        String.format("Default %s Constructor", getClass().getSimpleName()),
        getClass().getConstructors()[0]);

    MBeanAttributeInfo[] attributesInfo = new MBeanAttributeInfo[attributeInfoList.size()];
    attributesInfo = attributeInfoList.toArray(attributesInfo);

    if (description == null) {
      description = DEFAULT_DESCRIPTION;
    }

    _mBeanInfo = new MBeanInfo(getClass().getName(), description, attributesInfo,
        new MBeanConstructorInfo[]{constructorInfo}, new MBeanOperationInfo[0],
        new MBeanNotificationInfo[0]);

    // Update _attributeMap reference.
    _attributeMap = newAttributeMap;
  }

  /**
   * Call doRegister() to finish registration MBean and the attributes.
   */
  public abstract DynamicMBeanProvider register() throws JMException;

  /**
   * Unregister the MBean and clean up object name record.
   * Note that all the metric data is kept even after unregister.
   */
  public synchronized void unregister() {
    MBeanRegistrar.unregister(_objectName);
    _objectName = null;
  }

  @Override
  public Object getAttribute(String attribute) throws AttributeNotFoundException {
    if (SENSOR_NAME_TAG.equals(attribute)) {
      return getSensorName();
    }

    DynamicMetric metric = _attributeMap.get(attribute);
    if (metric == null) {
      throw new AttributeNotFoundException("Attribute[" + attribute + "] is not found.");
    }

    return metric.getAttributeValue(attribute);
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList attributeList = new AttributeList();
    for (String attributeName : attributes) {
      try {
        Object value = getAttribute(attributeName);
        attributeList.add(new Attribute(attributeName, value));
      } catch (AttributeNotFoundException ex) {
        _logger.error("Failed to get attribute: " + attributeName, ex);
      }
    }
    return attributeList;
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    return _mBeanInfo;
  }

  @Override
  public void setAttribute(Attribute attribute) {
    // All MBeans are readonly
    return;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    // All MBeans are readonly
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) {
    // No operation supported
    return null;
  }

  /**
   * NOTE: This method is not thread-safe nor atomic.
   * Increment the value of a given SimpleDynamicMetric by 1.
   */
  protected void incrementSimpleDynamicMetric(SimpleDynamicMetric<Long> metric) {
    incrementSimpleDynamicMetric(metric, 1);
  }

  /**
   * NOTE: This method is not thread-safe nor atomic.
   * Increment the value of a given SimpleDynamicMetric with input value.
   */
  protected void incrementSimpleDynamicMetric(SimpleDynamicMetric<Long> metric, long value) {
    metric.updateValue(metric.getValue() + value);
  }

  /**
   * Return the interval length for the underlying reservoir used by the MBean metric configured
   * in the system env variables. If not found, use default value.
   */
  protected Long getResetIntervalInMs() {
    return getSystemPropertyAsLong(HELIX_MONITOR_TIME_WINDOW_LENGTH_MS, DEFAULT_RESET_INTERVAL_MS);
  }

  /**
   * Get the value of system property
   * @param propertyKey
   * @param propertyDefaultValue
   * @return
   */
  private long getSystemPropertyAsLong(String propertyKey, long propertyDefaultValue) {
    String valueString = System.getProperty(propertyKey, "" + propertyDefaultValue);

    try {
      long value = Long.parseLong(valueString);
      if (value > 0) {
        return value;
      }
    } catch (NumberFormatException e) {
      _logger.warn("Exception while parsing property: " + propertyKey + ", string: " + valueString
          + ", using default value: " + propertyDefaultValue);
    }

    return propertyDefaultValue;
  }
}
