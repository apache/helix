package org.apache.helix.model;

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

import org.apache.helix.api.Scope;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.serializer.StringSerializer;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Configuration for a resource provisioner. This contains a ProvisionerConfig, which contains
 * information specific to each provisioner.
 */
public final class ProvisionerConfigHolder {
  private enum Fields {
    SERIALIZER_CLASS,
    PROVISIONER_CONFIG,
    PROVISIONER_CONFIG_CLASS
  }

  private static final Logger LOG = Logger.getLogger(ProvisionerConfigHolder.class);
  private StringSerializer _serializer;
  private Provisioner _provisioner;
  private final ProvisionerConfig _config;
  private final NamespacedConfig _backingConfig;

  /**
   * Instantiate a ProvisionerConfigHolder
   * @param config provisioner config
   */
  public ProvisionerConfigHolder(ProvisionerConfig config) {
    _backingConfig =
        new NamespacedConfig(Scope.resource(config.getResourceId()),
            ProvisionerConfigHolder.class.getSimpleName());
    _backingConfig.setSimpleField(Fields.SERIALIZER_CLASS.toString(), config.getSerializerClass()
        .getName());
    _backingConfig.setSimpleField(Fields.PROVISIONER_CONFIG_CLASS.toString(), config.getClass()
        .getName());
    _config = config;
    try {
      _serializer = config.getSerializerClass().newInstance();
      _backingConfig.setSimpleField(Fields.PROVISIONER_CONFIG.toString(),
          _serializer.serialize(config));
    } catch (InstantiationException e) {
      LOG.error("Error initializing the configuration", e);
    } catch (IllegalAccessException e) {
      LOG.error("Error initializing the configuration", e);
    }
  }

  /**
   * Instantiate from a physical ResourceConfiguration
   * @param resourceConfiguration populated ResourceConfiguration
   */
  public ProvisionerConfigHolder(ResourceConfiguration resourceConfiguration) {
    _backingConfig =
        new NamespacedConfig(resourceConfiguration, ProvisionerConfigHolder.class.getSimpleName());
    _serializer = getSerializer();
    _config = getConfig();
  }

  /**
   * Get the class that can serialize and deserialize the provisioner config
   * @return StringSerializer
   */
  private StringSerializer getSerializer() {
    String serializerClassName = _backingConfig.getSimpleField(Fields.SERIALIZER_CLASS.toString());
    if (serializerClassName != null) {
      try {
        return (StringSerializer) HelixUtil.loadClass(getClass(), serializerClassName)
            .newInstance();
      } catch (InstantiationException e) {
        LOG.error("Error getting the serializer", e);
      } catch (IllegalAccessException e) {
        LOG.error("Error getting the serializer", e);
      } catch (ClassNotFoundException e) {
        LOG.error("Error getting the serializer", e);
      }
    }
    return null;
  }

  private ProvisionerConfig getConfig() {
    String className = _backingConfig.getSimpleField(Fields.PROVISIONER_CONFIG_CLASS.toString());
    if (className != null) {
      try {
        Class<? extends ProvisionerConfig> configClass =
            HelixUtil.loadClass(getClass(), className).asSubclass(ProvisionerConfig.class);
        String serialized = _backingConfig.getSimpleField(Fields.PROVISIONER_CONFIG.toString());
        return _serializer.deserialize(configClass, serialized);
      } catch (ClassNotFoundException e) {
        LOG.error(className + " is not a valid class");
      } catch (ClassCastException e) {
        LOG.error("Could not convert the persisted data into a " + className, e);
      }
    }
    return null;
  }

  /**
   * Get a provisioner class instance
   * @return Provisioner
   */
  public Provisioner getProvisioner() {
    // cache the provisioner to avoid loading and instantiating it excessively
    if (_provisioner == null) {
      if (_config == null || _config.getProvisionerRef() == null) {
        return null;
      }
      _provisioner = _config.getProvisionerRef().getProvisioner();
    }
    return _provisioner;
  }

  /**
   * Get the instantiated ProvisionerConfig
   * @param configClass specific class of the ProvisionerConfig
   * @return ProvisionerConfig subclass instance, or null if conversion is not possible
   */
  public <T extends ProvisionerConfig> T getProvisionerConfig(Class<T> configClass) {
    if (_config != null) {
      try {
        return configClass.cast(_config);
      } catch (ClassCastException e) {
        LOG.warn(configClass + " is incompatible with config class: " + _config.getClass());
      }
    }
    return null;
  }

  /**
   * Get the provisioner config serialized as a string
   * @return string representing the config
   */
  public String getSerializedConfig() {
    return _backingConfig.getSimpleField(Fields.PROVISIONER_CONFIG.toString());
  }

  /**
   * Convert this to a namespaced config
   * @return NamespacedConfig
   */
  public NamespacedConfig toNamespacedConfig() {
    return _backingConfig;
  }

  /**
   * Get a ProvisionerConfig from a physical resource config
   * @param resourceConfiguration physical resource config
   * @return ProvisionerConfig
   */
  public static ProvisionerConfigHolder from(ResourceConfiguration resourceConfiguration) {
    return new ProvisionerConfigHolder(resourceConfiguration);
  }

  /**
   * Get a ProvisionerConfigHolder from a ProvisionerConfig
   * @param config instantiated ProvisionerConfig
   * @return ProvisionerConfigHolder
   */
  public static ProvisionerConfigHolder from(ProvisionerConfig config) {
    return new ProvisionerConfigHolder(config);
  }
}
