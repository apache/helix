package org.apache.helix.controller.rebalancer.context;

import org.apache.helix.api.NamespacedConfig;
import org.apache.helix.api.Scope;
import org.apache.helix.model.ResourceConfiguration;
import org.apache.helix.util.HelixUtil;
import org.apache.log4j.Logger;

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

/**
 * Configuration for a resource rebalancer. This contains a RebalancerContext, which contains
 * information specific to each rebalancer.
 */
public final class RebalancerConfig {
  private enum Fields {
    SERIALIZER_CLASS,
    REBALANCER_CONTEXT,
    REBALANCER_CONTEXT_CLASS
  }

  private static final Logger LOG = Logger.getLogger(RebalancerConfig.class);
  private ContextSerializer _serializer;
  private Rebalancer _rebalancer;
  private final RebalancerContext _context;
  private final NamespacedConfig _config;

  /**
   * Instantiate a RebalancerConfig
   * @param context rebalancer context
   * @param rebalancerRef reference to the rebalancer class that will be used
   */
  public RebalancerConfig(RebalancerContext context) {
    _config =
        new NamespacedConfig(Scope.resource(context.getResourceId()),
            RebalancerConfig.class.getSimpleName());
    _config.setSimpleField(Fields.SERIALIZER_CLASS.toString(), context.getSerializerClass()
        .getName());
    _config
        .setSimpleField(Fields.REBALANCER_CONTEXT_CLASS.toString(), context.getClass().getName());
    _context = context;
    try {
      _serializer = context.getSerializerClass().newInstance();
      _config.setSimpleField(Fields.REBALANCER_CONTEXT.toString(), _serializer.serialize(context));
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
  public RebalancerConfig(ResourceConfiguration resourceConfiguration) {
    _config = new NamespacedConfig(resourceConfiguration, RebalancerConfig.class.getSimpleName());
    _serializer = getSerializer();
    _context = getContext();
  }

  /**
   * Get the class that can serialize and deserialize the rebalancer context
   * @return ContextSerializer
   */
  private ContextSerializer getSerializer() {
    String serializerClassName = _config.getSimpleField(Fields.SERIALIZER_CLASS.toString());
    if (serializerClassName != null) {
      try {
        return (ContextSerializer) HelixUtil.loadClass(getClass(), serializerClassName)
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

  private RebalancerContext getContext() {
    String className = _config.getSimpleField(Fields.REBALANCER_CONTEXT_CLASS.toString());
    try {
      Class<? extends RebalancerContext> contextClass =
          HelixUtil.loadClass(getClass(), className).asSubclass(RebalancerContext.class);
      String serialized = _config.getSimpleField(Fields.REBALANCER_CONTEXT.toString());
      return _serializer.deserialize(contextClass, serialized);
    } catch (ClassNotFoundException e) {
      LOG.error(className + " is not a valid class");
    } catch (ClassCastException e) {
      LOG.error(className + " does not implement RebalancerContext");
    }
    return null;
  }

  /**
   * Get a rebalancer class instance
   * @return Rebalancer
   */
  public Rebalancer getRebalancer() {
    // cache the rebalancer to avoid loading and instantiating it excessively
    if (_rebalancer == null) {
      if (_context == null || _context.getRebalancerRef() == null) {
        return null;
      }
      _rebalancer = _context.getRebalancerRef().getRebalancer();
    }
    return _rebalancer;
  }

  /**
   * Get the instantiated RebalancerContext
   * @param contextClass specific class of the RebalancerContext
   * @return RebalancerContext subclass instance, or null if conversion is not possible
   */
  public <T extends RebalancerContext> T getRebalancerContext(Class<T> contextClass) {
    try {
      return contextClass.cast(_context);
    } catch (ClassCastException e) {
      LOG.info(contextClass + " is incompatible with context class: " + _context.getClass());
    }
    return null;
  }

  /**
   * Convert this to a namespaced config
   * @return NamespacedConfig
   */
  public NamespacedConfig toNamespacedConfig() {
    return _config;
  }
}
