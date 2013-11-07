package org.apache.helix.model;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.config.UserConfig;
import org.apache.helix.api.id.PartitionId;

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
 * Persisted configuration properties for a partition
 */
public class PartitionConfiguration extends HelixProperty {
  /**
   * Instantiate for an id
   * @param id partition id
   */
  public PartitionConfiguration(PartitionId id) {
    super(id.stringify());
  }

  /**
   * Instantiate from a record
   * @param record configuration properties
   */
  public PartitionConfiguration(ZNRecord record) {
    super(record);
  }

  /**
   * Create a new PartitionConfiguration from a UserConfig
   * @param userConfig user-defined configuration properties
   * @return PartitionConfiguration
   */
  public static PartitionConfiguration from(UserConfig userConfig) {
    PartitionConfiguration partitionConfiguration =
        new PartitionConfiguration(PartitionId.from(userConfig.getId()));
    partitionConfiguration.addNamespacedConfig(userConfig);
    return partitionConfiguration;
  }
}
