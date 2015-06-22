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

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfig;
import org.apache.helix.controller.rebalancer.config.RebalancerConfigHolder;
import org.apache.log4j.Logger;

import com.google.common.base.Enums;
import com.google.common.base.Optional;

/**
 * Persisted configuration properties for a resource
 */
public class ResourceConfiguration extends HelixProperty {
  private static final Logger LOG = Logger.getLogger(ResourceConfiguration.class);

  public enum Fields {
  }

  /**
   * Instantiate for an id
   * @param id resource id
   */
  public ResourceConfiguration(ResourceId id) {
    super(id.stringify());
  }

  /**
   * Get the resource that is rebalanced
   * @return resource id
   */
  public ResourceId getResourceId() {
    return ResourceId.from(getId());
  }

  /**
   * Instantiate from a record
   * @param record configuration properties
   */
  public ResourceConfiguration(ZNRecord record) {
    super(record);
  }

  /**
   * Get a backward-compatible resource user config
   * @return UserConfig
   */
  public UserConfig getUserConfig() {
    UserConfig userConfig = UserConfig.from(this);
    try {
      for (String simpleField : _record.getSimpleFields().keySet()) {
        Optional<Fields> enumField = Enums.getIfPresent(Fields.class, simpleField);
        Optional<HelixPropertyAttribute> superEnumField =
            Enums.getIfPresent(HelixPropertyAttribute.class, simpleField);
        if (!simpleField.contains(NamespacedConfig.PREFIX_CHAR + "") && !enumField.isPresent()
            && !superEnumField.isPresent()) {
          userConfig.setSimpleField(simpleField, _record.getSimpleField(simpleField));
        }
      }
      for (String listField : _record.getListFields().keySet()) {
        if (!listField.contains(NamespacedConfig.PREFIX_CHAR + "")) {
          userConfig.setListField(listField, _record.getListField(listField));
        }
      }
      for (String mapField : _record.getMapFields().keySet()) {
        if (!mapField.contains(NamespacedConfig.PREFIX_CHAR + "")) {
          userConfig.setMapField(mapField, _record.getMapField(mapField));
        }
      }
    } catch (NoSuchMethodError e) {
      LOG.error("Could not parse ResourceConfiguration", e);
    }
    return userConfig;
  }

  /**
   * Get a RebalancerConfig if available
   * @return RebalancerConfig, or null
   */
  public <T extends RebalancerConfig> T getRebalancerConfig(Class<T> clazz) {
    RebalancerConfigHolder config = new RebalancerConfigHolder(this);
    return config.getRebalancerConfig(clazz);
  }

  /**
   * Check if this resource config has a rebalancer config
   * @return true if a rebalancer config is attached, false otherwise
   */
  public boolean hasRebalancerConfig() {
    return _record.getSimpleFields().containsKey(
        RebalancerConfigHolder.class.getSimpleName() + NamespacedConfig.PREFIX_CHAR
            + RebalancerConfigHolder.Fields.REBALANCER_CONFIG);
  }

  /**
   * Get a ProvisionerConfig, if available
   * @param clazz the class to cast to
   * @return ProvisionerConfig, or null
   */
  public <T extends ProvisionerConfig> T getProvisionerConfig(Class<T> clazz) {
    ProvisionerConfigHolder configHolder = new ProvisionerConfigHolder(this);
    return configHolder.getProvisionerConfig(clazz);
  }
}
