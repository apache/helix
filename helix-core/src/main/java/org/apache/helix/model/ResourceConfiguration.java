package org.apache.helix.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.NamespacedConfig;
import org.apache.helix.api.PartitionId;
import org.apache.helix.api.RebalancerConfig;
import org.apache.helix.api.ResourceId;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;

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
 * Persisted configuration properties for a resource
 */
public class ResourceConfiguration extends HelixProperty {
  public enum Fields {
    PARTITION_LIST
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
   * Set the partitions for this resource
   * @param partitionIds list of partition ids
   */
  public void setPartitionIds(List<PartitionId> partitionIds) {
    _record.setListField(Fields.PARTITION_LIST.toString(),
        Lists.transform(partitionIds, Functions.toStringFunction()));
  }

  /**
   * Get the partitions for this resource
   * @return list of partition ids
   */
  public List<PartitionId> getPartitionIds() {
    List<String> partitionNames = _record.getListField(Fields.PARTITION_LIST.toString());
    if (partitionNames != null) {
      return Lists.transform(partitionNames, new Function<String, PartitionId>() {
        @Override
        public PartitionId apply(String partitionName) {
          return PartitionId.from(partitionName);
        }
      });
    }
    return null;
  }

  /**
   * Add a rebalancer config to this resource
   * @param config populated rebalancer config
   */
  public void addRebalancerConfig(RebalancerConfig config) {
    addNamespacedConfig(config);
    setPartitionIds(new ArrayList<PartitionId>(config.getPartitionSet()));
  }

  /**
   * Create a new ResourceConfiguration from a NamespacedConfig
   * @param namespacedConfig namespaced configuration properties
   * @return ResourceConfiguration
   */
  public static ResourceConfiguration from(NamespacedConfig namespacedConfig) {
    ResourceConfiguration resourceConfiguration =
        new ResourceConfiguration(ResourceId.from(namespacedConfig.getId()));
    resourceConfiguration.addNamespacedConfig(namespacedConfig);
    return resourceConfiguration;
  }

}
