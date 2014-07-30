package org.apache.helix.controller.rebalancer.config;

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
import org.apache.helix.controller.rebalancer.HelixRebalancer;
import org.apache.helix.controller.serializer.StringSerializer;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

/**
 * Configuration for a resource rebalancer. This contains a RebalancerConfig, which contains
 * information specific to each rebalancer.
 */
public final class RebalancerConfigHolder {
  public enum Fields {
    SERIALIZER_CLASS,
    REBALANCER_CONFIG,
    REBALANCER_CONFIG_CLASS
  }

  private static final Logger LOG = Logger.getLogger(RebalancerConfigHolder.class);
  private StringSerializer _serializer;
  private HelixRebalancer _rebalancer;
  private final RebalancerConfig _config;
  private final NamespacedConfig _backingConfig;

  /**
   * Instantiate a RebalancerConfig
   * @param config rebalancer config
   * @param rebalancerRef reference to the rebalancer class that will be used
   */
  public RebalancerConfigHolder(RebalancerConfig config) {
    _backingConfig =
        new NamespacedConfig(Scope.resource(config.getResourceId()),
            RebalancerConfigHolder.class.getSimpleName());
    _backingConfig.setSimpleField(Fields.SERIALIZER_CLASS.toString(), config.getSerializerClass()
        .getName());
    _backingConfig.setSimpleField(Fields.REBALANCER_CONFIG_CLASS.toString(), config.getClass()
        .getName());
    _config = config;
    try {
      _serializer = config.getSerializerClass().newInstance();
      _backingConfig.setSimpleField(Fields.REBALANCER_CONFIG.toString(),
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
  public RebalancerConfigHolder(ResourceConfiguration resourceConfiguration) {
    _backingConfig =
        new NamespacedConfig(resourceConfiguration, RebalancerConfigHolder.class.getSimpleName());
    _serializer = getSerializer();
    _config = getConfig();
  }

  /**
   * Get the class that can serialize and deserialize the rebalancer config
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

  private RebalancerConfig getConfig() {
    String className = _backingConfig.getSimpleField(Fields.REBALANCER_CONFIG_CLASS.toString());
    if (className != null) {
      try {
        Class<? extends RebalancerConfig> configClass =
            HelixUtil.loadClass(getClass(), className).asSubclass(RebalancerConfig.class);
        String serialized = _backingConfig.getSimpleField(Fields.REBALANCER_CONFIG.toString());
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
   * Get the instantiated RebalancerConfig
   * @param configClass specific class of the RebalancerConfig
   * @return RebalancerConfig subclass instance, or null if conversion is not possible
   */
  public <T extends RebalancerConfig> T getRebalancerConfig(Class<T> configClass) {
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
   * Get the rebalancer config serialized as a string
   * @return string representing the config
   */
  public String getSerializedConfig() {
    return _backingConfig.getSimpleField(Fields.REBALANCER_CONFIG.toString());
  }

  /**
   * Convert this to a namespaced config
   * @return NamespacedConfig
   */
  public NamespacedConfig toNamespacedConfig() {
    return _backingConfig;
  }

  /**
   * Get a RebalancerConfig from a physical resource config
   * @param resourceConfiguration physical resource config
   * @return RebalancerConfig
   */
  public static RebalancerConfigHolder from(ResourceConfiguration resourceConfiguration) {
    return new RebalancerConfigHolder(resourceConfiguration);
  }

  /**
   * Get a RebalancerConfigHolder from a RebalancerConfig
   * @param config instantiated RebalancerConfig
   * @return RebalancerConfigHolder
   */
  public static RebalancerConfigHolder from(RebalancerConfig config) {
    return new RebalancerConfigHolder(config);
  }
}
