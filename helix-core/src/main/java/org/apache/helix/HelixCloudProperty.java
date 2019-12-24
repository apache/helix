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
 * hold helix cloud properties
 */
public class HelixCloudProperty {
  private static final Logger logger = LoggerFactory.getLogger(HelixCloudProperty.class.getName());
  private static final String AZURE_CLOUD_PROPERTY_FILE = SystemPropertyKeys.AZURE_CLOUD_PROPERTIES;

  private boolean _isCloudEnabled;
  private String _cloudId;
  private String _cloudProvider;
  private List<String> _cloudInfoSources;
  private String _cloudInfoProcessorName;
  private int _maxRetry;
  private long _cloudConnectionTimeout;
  private long _cloudRequestTimeout;
  private Properties _customizedProperties = new Properties();

  /**
   * Initialize Helix Cloud Property based on the provider
   * @param
   */
  public HelixCloudProperty(CloudConfig cloudConfig) {
    setCloudEndabled(cloudConfig.isCloudEnabled());
    setCloudId(cloudConfig.getCloudID());
    setCloudProvider(cloudConfig.getCloudProvider());
    switch (cloudConfig.getCloudProvider()) {
      case "AZURE":
        Properties azureProperties = new Properties();
        try {
          InputStream stream =
              Thread.currentThread().getContextClassLoader().getResourceAsStream(AZURE_CLOUD_PROPERTY_FILE);
          azureProperties.load(stream);
        } catch (Exception e) {
          String errMsg = "failed to open properties file: " + AZURE_CLOUD_PROPERTY_FILE;
          throw new IllegalArgumentException(errMsg, e);
        }
        logger.info("load helix Azure cloud properties: " + azureProperties);
        setCloudInfoSources(Collections.singletonList(azureProperties.getProperty("CLOUD_INFO_SOURCE")));
        setCloudInfoProcessorName(azureProperties.getProperty("CLOUD_INFO_PROCESSFOR_NAME"));
        setCloudMaxRetry(azureProperties.getProperty("RETRY_MAX"));
        setCloudConnectionTimeout(azureProperties.getProperty("CONNECTION_TIMEOUT_MS"));
        setCloudRequestTimeout(azureProperties.getProperty("REQUEST_TIMEOUT_MS"));
      case "CUSTOMIZED":
        setCloudInfoSources(cloudConfig.getCloudInfoSources());
        setCloudInfoProcessorName(cloudConfig.getCloudInfoProcessorName());
      default:
        throw new IllegalArgumentException("unrecognized cloud provider: " + cloudConfig.getCloudProvider());
    }
  }

  public boolean getCloudEnabled() {
    return _isCloudEnabled;
  }

  public String getCloudId() {
    return _cloudId;
  }

  public String getCloudProvider() {
    return _cloudProvider;
  }

  public List<String> getCloudInfoSources() {
    return _cloudInfoSources;
  }

  public String getCloudInfoProcessorName() {
    return _cloudInfoProcessorName;
  }

  public int getMaxRetry() {
    return _maxRetry;
  }

  public long getCloudConnectionTimeout() {
    return _cloudConnectionTimeout;
  }

  public long getCloudRequestTimeout() {
    return _cloudRequestTimeout;
  }

  public Properties getCustomizedProperties() {
    return _customizedProperties;
  }

  public void setCloudEndabled(boolean isCloudEnabled) {
    _isCloudEnabled = isCloudEnabled;
  }

  public void setCloudId(String cloudId) {
    _cloudId = cloudId;
  }

  public void setCloudProvider(String cloudProvider) {
    _cloudProvider = cloudProvider;
  }

  public void setCloudInfoSources(List<String> sources) {
    _cloudInfoSources = sources;
  }

  public void setCloudInfoProcessorName(String cloudInfoProcessorName) {
    _cloudInfoProcessorName = cloudInfoProcessorName;
  }

  public void setCloudMaxRetry(String maxRetry) {
    _maxRetry = Integer.valueOf(maxRetry);
  }

  public void setCloudConnectionTimeout(String cloudConnectionTimeout) {
    _cloudConnectionTimeout = Long.valueOf(cloudConnectionTimeout);
  }

  public void setCloudRequestTimeout(String cloudRequestTimeout) {
    _cloudRequestTimeout = Long.valueOf(cloudRequestTimeout);
  }

  public void setCustomizedCloudProperties(Properties customizedCloudProperties) {
    _customizedProperties.putAll(customizedCloudProperties);
  }
}
