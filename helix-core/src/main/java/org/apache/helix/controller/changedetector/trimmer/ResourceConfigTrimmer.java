package org.apache.helix.controller.changedetector.trimmer;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.ResourceConfig;
import org.apache.helix.model.ResourceConfig.ResourceConfigProperty;

/**
 * A singleton HelixProperty Trimmer for ResourceConfig to remove the non-cluster-topology-related
 * fields.
 */
public class ResourceConfigTrimmer extends HelixPropertyTrimmer<ResourceConfig> {
  private static final ResourceConfigTrimmer _resourceConfigTrimmer = new ResourceConfigTrimmer();

  /**
   * The following fields are considered as non-topology related.
   * MONITORING_DISABLED,
   * GROUP_ROUTING_ENABLED,
   * EXTERNAL_VIEW_DISABLED,
   * DELAY_REBALANCE_ENABLED,
   * HELIX_ENABLED
   */
  private static final Map<FieldType, Set<String>> STATIC_NON_TRIMMABLE_FIELD_MAP = ImmutableMap
      .of(FieldType.SIMPLE_FIELD, ImmutableSet
              .of(ResourceConfigProperty.NUM_PARTITIONS.name(),
                  ResourceConfigProperty.STATE_MODEL_DEF_REF.name(),
                  ResourceConfigProperty.STATE_MODEL_FACTORY_NAME.name(),
                  ResourceConfigProperty.MIN_ACTIVE_REPLICAS.name(),
                  ResourceConfigProperty.REPLICAS.name(),
                  ResourceConfigProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
                  ResourceConfigProperty.INSTANCE_GROUP_TAG.name(),
                  ResourceConfigProperty.RESOURCE_GROUP_NAME.name(),
                  ResourceConfigProperty.RESOURCE_TYPE.name()),
          FieldType.MAP_FIELD, ImmutableSet
              .of(ResourceConfigProperty.PARTITION_CAPACITY_MAP.name()));

  private ResourceConfigTrimmer() {
  }

  @Override
  protected Map<FieldType, Set<String>> getNonTrimmableFields(ResourceConfig resourceConfig) {
    Map<FieldType, Set<String>> nonTrimmableFields = new HashMap<>(STATIC_NON_TRIMMABLE_FIELD_MAP);
    // Also don't trim the application defined preference list in the list fields.
    // They are fixed and considered as part of the cluster topology.
    nonTrimmableFields
        .put(FieldType.LIST_FIELD, resourceConfig.getRecord().getListFields().keySet());
    return nonTrimmableFields;
  }

  @Override
  public ResourceConfig trimProperty(ResourceConfig property) {
    return new ResourceConfig(doTrim(property));
  }

  public static ResourceConfigTrimmer getInstance() {
    return _resourceConfigTrimmer;
  }
}
