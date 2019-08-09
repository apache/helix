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

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanConstructorInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.helix.monitoring.SensorNameProvider;
import org.apache.helix.monitoring.mbeans.MBeanRegistrar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dynamic MBean provider that reporting DynamicMetric attributes
 */
public abstract class DynamicMBeanProvider implements DynamicMBean, SensorNameProvider {
  protected final Logger _logger = LoggerFactory.getLogger(getClass());
  protected static final long DEFAULT_RESET_INTERVAL_MS = 60 * 60 * 1000; // Reset time every hour
  protected static final String RESET_INTERVAL_SYSTEM_PROPERTY_KEY = "reservoir.length.ms";
  private static String SENSOR_NAME_TAG = "SensorName";
  private static String DEFAULT_DESCRIPTION =
      "Information on the management interface of the MBean";

  // Attribute name to the DynamicMetric object mapping
  private final Map<String, DynamicMetric> _attributeMap = new HashMap<>();
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
    updateAttributtInfos(dynamicMetrics, description);
    _objectName = MBeanRegistrar.register(this, objectName);
    return true;
  }

  protected synchronized boolean doRegister(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      ObjectName objectName) throws JMException {
    return doRegister(dynamicMetrics, null, objectName);
  }

  /**
   * Update the Dynamic MBean provider with new metric list.
   * @param description description of the MBean
   * @param dynamicMetrics the DynamicMetrics
   */
  private void updateAttributtInfos(Collection<DynamicMetric<?, ?>> dynamicMetrics,
      String description) {
    _attributeMap.clear();

    // get all attributes that can be emit by the dynamicMetrics.
    List<MBeanAttributeInfo> attributeInfoList = new ArrayList<>();
    if (dynamicMetrics != null) {
      for (DynamicMetric dynamicMetric : dynamicMetrics) {
        Iterator<MBeanAttributeInfo> iter = dynamicMetric.getAttributeInfos().iterator();
        while (iter.hasNext()) {
          MBeanAttributeInfo attributeInfo = iter.next();
          // Info list to create MBean info
          attributeInfoList.add(attributeInfo);
          // Attribute mapping for getting attribute value when getAttribute() is called
          _attributeMap.put(attributeInfo.getName(), dynamicMetric);
        }
      }
    }

    // SensorName
    attributeInfoList.add(new MBeanAttributeInfo(SENSOR_NAME_TAG, String.class.getName(),
        "The name of the metric sensor", true, false, false));

    MBeanConstructorInfo constructorInfo = new MBeanConstructorInfo(
        String.format("Default %s Constructor", getClass().getSimpleName()),
        getClass().getConstructors()[0]);

    MBeanAttributeInfo[] attributeInfos = new MBeanAttributeInfo[attributeInfoList.size()];
    attributeInfos = attributeInfoList.toArray(attributeInfos);

    if (description == null) {
      description = DEFAULT_DESCRIPTION;
    }

    _mBeanInfo = new MBeanInfo(getClass().getName(), description, attributeInfos,
        new MBeanConstructorInfo[] {
            constructorInfo
        }, new MBeanOperationInfo[0], new MBeanNotificationInfo[0]);
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
  public Object getAttribute(String attribute)
      throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (SENSOR_NAME_TAG.equals(attribute)) {
      return getSensorName();
    }

    if (!_attributeMap.containsKey(attribute)) {
      return null;
    }

    return _attributeMap.get(attribute).getAttributeValue(attribute);
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList attributeList = new AttributeList();
    for (String attributeName : attributes) {
      try {
        Object value = getAttribute(attributeName);
        attributeList.add(new Attribute(attributeName, value));
      } catch (AttributeNotFoundException | MBeanException | ReflectionException ex) {
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
  public void setAttribute(Attribute attribute) throws AttributeNotFoundException,
      InvalidAttributeValueException, MBeanException, ReflectionException {
    // All MBeans are readonly
    return;
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    // All MBeans are readonly
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature)
      throws MBeanException, ReflectionException {
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
}
