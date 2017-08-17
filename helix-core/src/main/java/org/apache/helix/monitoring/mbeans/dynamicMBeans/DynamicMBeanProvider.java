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

import org.apache.helix.monitoring.SensorNameProvider;
import org.apache.log4j.Logger;

import javax.management.*;
import java.util.*;

/**
 * Dynamic MBean provider that reporting DynamicMetric attributes
 */
public class DynamicMBeanProvider implements DynamicMBean, SensorNameProvider {
  private static String SENSOR_NAME_TAG = "SensorName";
  private static final Logger _logger = Logger.getLogger(DynamicMBeanProvider.class);

  // Attribute name to the DynamicMetric object mapping
  private Map<String, DynamicMetric> _attributeMap = new HashMap<>();
  private MBeanInfo _mBeanInfo = null;
  private String _sensorName;

  /**
   * Instantiates a new Dynamic MBean provider.
   *
   * @param sensorName     the sensor name
   * @param description    the MBean description
   * @param dynamicMetrics the DynamicMetrics
   */
  public DynamicMBeanProvider(String sensorName, String description,
      Collection<DynamicMetric<?, ?>> dynamicMetrics) {
    _sensorName = sensorName;

    // get all attributes that can be emit by the dynamicMetrics.
    List<MBeanAttributeInfo> attributeInfoList = new ArrayList<>();
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

    // SensorName
    attributeInfoList.add(new MBeanAttributeInfo(SENSOR_NAME_TAG, String.class.getName(),
        "The name of the metric sensor", true, false, false));

    MBeanConstructorInfo constructorInfo = new MBeanConstructorInfo(
        String.format("Default %s Constructor", getClass().getSimpleName()),
        getClass().getConstructors()[0]);

    MBeanAttributeInfo[] attributeInfos = new MBeanAttributeInfo[attributeInfoList.size()];
    attributeInfos = attributeInfoList.toArray(attributeInfos);

    _mBeanInfo = new MBeanInfo(getClass().getName(), description, attributeInfos,
        new MBeanConstructorInfo[] { constructorInfo }, new MBeanOperationInfo[0],
        new MBeanNotificationInfo[0]);
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
  public void setAttribute(Attribute attribute)
      throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException,
      ReflectionException {
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

  @Override
  public String getSensorName() {
    return _sensorName;
  }
}
