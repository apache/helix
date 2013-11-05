package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.NamespacedConfig;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.manager.zk.ZKHelixManager;

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
 * Persisted configuration properties for a cluster
 */
public class ClusterConfiguration extends HelixProperty {
  /**
   * Instantiate for an id
   * @param id cluster id
   */
  public ClusterConfiguration(ClusterId id) {
    super(id.stringify());
  }

  /**
   * Instantiate from a record
   * @param record configuration properties
   */
  public ClusterConfiguration(ZNRecord record) {
    super(record);
  }

  /**
   * Get a typed cluster id
   * @return ClusterId
   */
  public ClusterId getClusterId() {
    return ClusterId.from(getId());
  }

  /**
   * Determine if participants can automatically join the cluster
   * @return true if allowed, false if disallowed
   */
  public boolean autoJoinAllowed() {
    return _record.getBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, false);
  }

  /**
   * Set if participants can automatically join the cluster
   * @param autoJoinAllowed true if allowed, false if disallowed
   */
  public void setAutoJoinAllowed(boolean autoJoinAllowed) {
    _record.setBooleanField(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, autoJoinAllowed);
  }

  /**
   * Get a backward-compatible cluster user config
   * @return UserConfig
   */
  public UserConfig getUserConfig() {
    UserConfig userConfig = UserConfig.from(this);
    for (String simpleField : _record.getSimpleFields().keySet()) {
      if (!simpleField.contains(NamespacedConfig.PREFIX_CHAR + "")
          && !simpleField.equals(ZKHelixManager.ALLOW_PARTICIPANT_AUTO_JOIN)) {
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
    return userConfig;
  }

  /**
   * Create a new ClusterConfiguration from a UserConfig
   * @param userConfig user-defined configuration properties
   * @return ClusterConfiguration
   */
  public static ClusterConfiguration from(UserConfig userConfig) {
    ClusterConfiguration clusterConfiguration =
        new ClusterConfiguration(ClusterId.from(userConfig.getId()));
    clusterConfiguration.addNamespacedConfig(userConfig);
    return clusterConfiguration;
  }
}
