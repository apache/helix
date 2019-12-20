package org.apache.helix;

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

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * hold helix-manager properties read from
 * helix-core/src/main/resources/cluster-manager.properties
 */
public class HelixParticipantProperties {
  private static final Logger LOG = LoggerFactory.getLogger(HelixParticipantProperties.class.getName());

  private String _cloudInfoProcessorName;
  private String _maxRetry;
  private Properties _properties;


  /**
   * Initialize properties from a file
   * @param
   */
  public HelixParticipantProperties(Properties properties) {
    _properties = properties;
    setCloudInfoProcessorName(_properties.getProperty("name"));
  }

  /**
   * Get properties wrapped as {@link Properties}
   * @return Properties
   */
  public Properties getProperties() {
    return _properties;
  }

  public String getCloudInfoProcessorName() {
    return _cloudInfoProcessorName;
  }

  public String getMaxRetry() {
    return _maxRetry;
  }

  public void setCloudInfoSources(List<String> sources) {

  }

  public void setCloudInfoProcessorName(String name) {

  }

  public void setCustomizedProperties(Properties properties) {
    _properties.putAll(properties);
  }

  /**
   * get property for key
   * @param key
   * @return property associated by key
   */
  public String getProperty(String key) {
    String value = _properties.getProperty(key);
    if (value == null) {
      LOG.warn("no property for key: " + key);
    }

    return value;
  }
}
