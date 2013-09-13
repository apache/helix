package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.ClusterId;
import org.apache.helix.api.Id;
import org.apache.helix.api.UserConfig;

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
   * Create a new ClusterConfiguration from a UserConfig
   * @param userConfig user-defined configuration properties
   * @return ClusterConfiguration
   */
  public static ClusterConfiguration from(UserConfig userConfig) {
    ClusterConfiguration clusterConfiguration =
        new ClusterConfiguration(Id.cluster(userConfig.getId()));
    clusterConfiguration.addNamespacedConfig(userConfig);
    return clusterConfiguration;
  }
}
