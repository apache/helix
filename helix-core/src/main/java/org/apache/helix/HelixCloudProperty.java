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

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.helix.cloud.constants.CloudProvider;
import org.apache.helix.cloud.event.CloudEventHandler;
import org.apache.helix.cloud.event.helix.CloudEventCallbackProperty;
import org.apache.helix.model.CloudConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hold helix cloud properties read from CloudConfig and user defined files. Clients may override
 * the fields from their application.
 */
public class HelixCloudProperty {
  private static final Logger LOG = LoggerFactory.getLogger(HelixCloudProperty.class.getName());
  private static final String AZURE_CLOUD_PROPERTY_FILE = SystemPropertyKeys.AZURE_CLOUD_PROPERTIES;
  private static final String DEFAULT_CLOUD_PROCESSOR_PACKAGE_PREFIX = "org.apache.helix.cloud.";
  private static final String CLOUD_INFO_SOURCE = "cloud_info_source";
  private static final String CLOUD_INFO_PROCESSOR_NAME = "cloud_info_processor_name";
  private static final String CLOUD_MAX_RETRY = "cloud_max_retry";
  private static final String CONNECTION_TIMEOUT_MS = "connection_timeout_ms";
  private static final String REQUEST_TIMEOUT_MS = "request_timeout_ms";

  // Denote whether the instance is considered as in a cloud environment.
  private boolean _isCloudEnabled;

  // Unique id of the cloud environment where the instance is in.
  private String _cloudId;

  // Cloud environment provider, e.g. Azure, AWS, GCP, etc.
  private String _cloudProvider;

  // The sources where the cloud instance information can be retrieved from.
  private List<String> _cloudInfoSources;

  // The name of the function that will fetch and parse cloud instance information.
  private String _cloudInfoProcessorName;

  // The package for the class which contains implementation to fetch
  // and parse cloud instance information.
  private String _cloudInfoProcessorPackage;

  // Http max retry times when querying the cloud instance information from cloud environment.
  private int _cloudMaxRetry;

  // Http connection time when querying the cloud instance information from cloud environment.
  private long _cloudConnectionTimeout;

  // Http request timeout when querying the cloud instance information from cloud environment.
  private long _cloudRequestTimeout;

  // Other customized properties that may be used.
  private final Properties _customizedCloudProperties = new Properties();

  private boolean _isCloudEventCallbackEnabled;

  private CloudEventCallbackProperty _cloudEventCallbackProperty;

  /**
   * Initialize Helix Cloud Property based on the provider
   * @param
   */
  public HelixCloudProperty(CloudConfig cloudConfig) {
    populateFieldsWithCloudConfig(cloudConfig);
  }

  public void populateFieldsWithCloudConfig(CloudConfig cloudConfig) {
    if (cloudConfig == null) {
      cloudConfig = new CloudConfig();
    }
    setCloudEnabled(cloudConfig.isCloudEnabled());
    setCloudId(cloudConfig.getCloudID());
    String cloudProviderStr = cloudConfig.getCloudProvider();
    setCloudProvider(cloudProviderStr);
    if (cloudProviderStr != null) {
      String cloudInfoProcessorName = null;
      switch (CloudProvider.valueOf(cloudProviderStr)) {
        case AZURE:
          Properties azureProperties = new Properties();
          try {
            InputStream stream = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream(AZURE_CLOUD_PROPERTY_FILE);
            azureProperties.load(stream);
          } catch (IOException e) {
            String errMsg =
                "failed to open Helix Azure cloud properties file: " + AZURE_CLOUD_PROPERTY_FILE;
            throw new IllegalArgumentException(errMsg, e);
          }
          LOG.info("Successfully loaded Helix Azure cloud properties: {}", azureProperties);
          setCloudInfoSources(
              Collections.singletonList(azureProperties.getProperty(CLOUD_INFO_SOURCE)));
          setCloudInfoProcessorPackage(
              DEFAULT_CLOUD_PROCESSOR_PACKAGE_PREFIX + cloudProviderStr.toLowerCase());
          setCloudInfoProcessorName(azureProperties.getProperty(CLOUD_INFO_PROCESSOR_NAME));
          setCloudMaxRetry(Integer.valueOf(azureProperties.getProperty(CLOUD_MAX_RETRY)));
          setCloudConnectionTimeout(
              Long.valueOf(azureProperties.getProperty(CONNECTION_TIMEOUT_MS)));
          setCloudRequestTimeout(Long.valueOf(azureProperties.getProperty(REQUEST_TIMEOUT_MS)));
          break;
        case CUSTOMIZED:
          setCloudInfoSources(cloudConfig.getCloudInfoSources());
          // Although it is unlikely that cloudInfoProcessorPackage is null, when using the CUSTOMIZED
          // cloud provider, we will set the processor package to helix cloud package to preserves the
          // backwards compatibility.
          setCloudInfoProcessorPackage(cloudConfig.getCloudInfoProcessorPackage() != null
              ? cloudConfig.getCloudInfoProcessorPackage()
              : DEFAULT_CLOUD_PROCESSOR_PACKAGE_PREFIX + cloudProviderStr.toLowerCase());
          setCloudInfoProcessorName(cloudConfig.getCloudInfoProcessorName());
          break;
        default:
          throw new HelixException(
              String.format("Unsupported cloud provider: %s", cloudConfig.getCloudProvider()));
      }
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

  /**
   * Get the package containing the CloudInfoProcessor class.
   * @return A package.
   */
  public String getCloudInfoProcessorPackage() {
    return _cloudInfoProcessorPackage;
  }

  public String getCloudInfoProcessorName() {
    return _cloudInfoProcessorName;
  }

  /**
   * Get the fully qualified class name for the class which contains implementation to fetch
   * and parse cloud instance information.
   * @return A fully qualified class name.
   */
  public String getCloudInfoProcessorFullyQualifiedClassName() {
    return _cloudInfoProcessorPackage + "." + _cloudInfoProcessorName;
  }

  public int getCloudMaxRetry() {
    return _cloudMaxRetry;
  }

  public long getCloudConnectionTimeout() {
    return _cloudConnectionTimeout;
  }

  public long getCloudRequestTimeout() {
    return _cloudRequestTimeout;
  }

  public Properties getCustomizedCloudProperties() {
    return _customizedCloudProperties;
  }

  public String getCloudEventHandlerClassName() {
    String defaultHandler = CloudEventHandler.class.getName();
    return getCloudEventCallbackProperty() == null ? defaultHandler
        : getCloudEventCallbackProperty().getUserArgs().getOrDefault(
            CloudEventCallbackProperty.UserArgsInputKey.CLOUD_EVENT_HANDLER_CLASS_NAME,
            defaultHandler);
  }

  public void setCloudEnabled(boolean isCloudEnabled) {
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

  /**
   * Set the package containing the class name of the cloud info processor.
   * @param cloudInfoProcessorPackage
   */
  private void setCloudInfoProcessorPackage(String cloudInfoProcessorPackage) {
    _cloudInfoProcessorPackage = cloudInfoProcessorPackage;
  }

  public void setCloudInfoProcessorName(String cloudInfoProcessorName) {
    _cloudInfoProcessorName = cloudInfoProcessorName;
  }

  public void setCloudMaxRetry(int cloudMaxRetry) {
    _cloudMaxRetry = cloudMaxRetry;
  }

  public void setCloudConnectionTimeout(long cloudConnectionTimeout) {
    _cloudConnectionTimeout = cloudConnectionTimeout;
  }

  public void setCloudRequestTimeout(long cloudRequestTimeout) {
    _cloudRequestTimeout = cloudRequestTimeout;
  }

  public void setCustomizedCloudProperties(Properties customizedCloudProperties) {
    _customizedCloudProperties.putAll(customizedCloudProperties);
  }

  public boolean isCloudEventCallbackEnabled() {
    return _isCloudEventCallbackEnabled;
  }

  public void setCloudEventCallbackEnabled(boolean enabled) {
    _isCloudEventCallbackEnabled = enabled;
  }

  public CloudEventCallbackProperty getCloudEventCallbackProperty() {
    return _cloudEventCallbackProperty;
  }

  public void setCloudEventCallbackProperty(CloudEventCallbackProperty cloudEventCallbackProperty) {
    _cloudEventCallbackProperty = cloudEventCallbackProperty;
  }
}
