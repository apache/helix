package org.apache.helix.model;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.ClusterId;

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
  private enum Fields {
    WRITE_ID
  }

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
   * Determine if participants can automatically join the cluster
   * @return true if allowed, false if disallowed
   */
  public boolean autoJoinAllowed() {
    return _record.getBooleanField(HelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, false);
  }

  /**
   * Set if participants can automatically join the cluster
   * @param autoJoinAllowed true if allowed, false if disallowed
   */
  public void setAutoJoinAllowed(boolean autoJoinAllowed) {
    _record.setBooleanField(HelixManager.ALLOW_PARTICIPANT_AUTO_JOIN, autoJoinAllowed);
  }

  /**
   * Set the identifier of this configuration for the last write
   * @param writeId positive random long identifier
   */
  public void setWriteId(long writeId) {
    _record.setLongField(Fields.WRITE_ID.toString(), writeId);
  }

  /**
   * Get the identifier for the last write to this configuration
   * @return positive write identifier, or -1 of not set
   */
  public long getWriteId() {
    return _record.getLongField(Fields.WRITE_ID.toString(), -1);
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
