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
import java.util.Properties;

import org.apache.helix.model.CloudConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Hold helix cloud properties coming from different sources.
 */
public class HelixCloudProperties {
  private static final Logger LOG = LoggerFactory.getLogger(HelixCloudProperties.class.getName());

  private final Properties _properties = new Properties();

  /**
   * Initialize helix cloud properties
   * To combine cloud properties from different sources, firstly, the cloud config will be examined, if the provider has its
   * properties defined in Helix, then the corresponding file under helix-core/src/main/resources/ will be read. Finally,
   * user input properties will be merged. If there is any duplicate key existing, the user defined property will have
   * higher priority and override other properties
   * @param userProperties
   * @param cloudConfig
   */
  public HelixCloudProperties(Properties userProperties, CloudConfig cloudConfig) {
    switch (cloudConfig.getCloudProvider()) {
      case "AZURE":
        // put cloud config in Zookeeper to local properties
        _properties
            .put(CloudConfig.CloudConfigProperty.CLOUD_ENABLED, cloudConfig.isCloudEnabled());
        _properties.put(CloudConfig.CloudConfigProperty.CLOUD_ID, cloudConfig.getCloudID());
        _properties
            .put(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER, cloudConfig.getCloudProvider());

        // put properties in Azure cloud properties files to local properties
        String filePath = SystemPropertyKeys.AZURE_CLOUD_PROPERTIES;
        Properties propertiesFromFile = new Properties();
        try {
          InputStream stream =
              Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
          propertiesFromFile.load(stream);
        } catch (Exception e) {
          String errMsg = "fail to open properties file: " + filePath;
          throw new IllegalArgumentException(errMsg, e);
        }
        _properties.putAll(propertiesFromFile);

        // load user defined properties to local properties
        _properties.putAll(userProperties);
        LOG.info("load helix Azure cloud properties: " + _properties);
      case "CUSTOMIZED":
        _properties
            .put(CloudConfig.CloudConfigProperty.CLOUD_ENABLED, cloudConfig.isCloudEnabled());
        _properties.put(CloudConfig.CloudConfigProperty.CLOUD_ID, cloudConfig.getCloudID());
        _properties
            .put(CloudConfig.CloudConfigProperty.CLOUD_PROVIDER, cloudConfig.getCloudProvider());
        _properties.put(CloudConfig.CloudConfigProperty.CLOUD_INFO_SOURCE,
            cloudConfig.getCloudInfoSources());
        _properties.put(CloudConfig.CloudConfigProperty.CLOUD_INFO_PROCESSOR_NAME,
            cloudConfig.getCloudInfoProcessorName());
        _properties.putAll(userProperties);
        LOG.info("load helix customized cloud properties: " + _properties);
    }
  }

  /**
   * Get helix cloud properties wrapped as {@link Properties}
   * @return Properties
   */
  public Properties getProperties() {
    return _properties;
  }

  /**
   * get helix cloud property for a key
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
