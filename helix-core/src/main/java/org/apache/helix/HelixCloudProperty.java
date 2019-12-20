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
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.helix.model.CloudConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * hold helix-manager properties read from
 * helix-core/src/main/resources/cluster-manager.properties
 */
public class HelixCloudProperty {
  private static final Logger LOG = LoggerFactory.getLogger(HelixCloudProperty.class.getName());
  private final String AZURE_CLOUD_PROPERTY_FILE = SystemPropertyKeys.AZURE_CLOUD_PROPERTIES;

  private Boolean _isCloudEnabled;
  private String _cloudId;
  private String _cloudProvider;
  private String _cloudInfoProcessorName;
  private String _maxRetry;
  private String _cloudConnectionTimeout;
  private String _cloudRequestTimeout;

  private Properties _properties;

  /**
   * Initialize Helix Participant Property
   * @param
   */
  public HelixCloudProperty(Properties properties, CloudConfig cloudConfig) {
    _properties = properties;

    setCloudEndabled(cloudConfig.isCloudEnabled());
    setCloudId(cloudConfig.getCloudID());
    setCloudProvider(cloudConfig.getCloudProvider());
    switch (cloudConfig.getCloudProvider()) {
      case "AZURE":

        // put properties in Azure cloud properties files to local properties
        Properties propertiesFromFile = new Properties();
        try {
          InputStream stream =
              Thread.currentThread().getContextClassLoader().getResourceAsStream(AZURE_CLOUD_PROPERTY_FILE);
          propertiesFromFile.load(stream);
        } catch (Exception e) {
          String errMsg = "fail to open properties file: " + AZURE_CLOUD_PROPERTY_FILE;
          throw new IllegalArgumentException(errMsg, e);
        }

        LOG.info("load helix Azure cloud properties: " + _properties);
        setCloudInfoSources(Collections.singletonList(propertiesFromFile.getProperty("sources")));
        setCloudInfoProcessorName(propertiesFromFile.getProperty("name"));
      case "CUSTOMIZED":
        setCloudInfoSources(cloudConfig.getCloudInfoSources());
        setCloudInfoProcessorName(cloudConfig.getCloudInfoProcessorName());
        LOG.info("load helix customized cloud properties: " + _properties);
    }

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

  public void setCloudEndabled(Boolean isCloudEnabled) {
    _isCloudEnabled = isCloudEnabled;
  }

  public void setCloudId(String cloudId) {
    _cloudId = cloudId;
  }

  public void setCloudProvider(String cloudProvider) {
    _cloudProvider = cloudProvider;
  }

  public void setCloudInfoSources(List<String> sources) {

  }

  public void setCloudInfoProcessorName(String name) {

  }

  public void setCloudMaxRetry(String number) {

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
