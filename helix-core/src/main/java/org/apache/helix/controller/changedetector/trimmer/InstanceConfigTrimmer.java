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

import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.InstanceConfig.InstanceConfigProperty;

/**
 * A singleton HelixProperty Trimmer for InstanceConfig to remove the non-cluster-topology-related
 * fields.
 */
public class InstanceConfigTrimmer extends HelixPropertyTrimmer<InstanceConfig> {
  private static final InstanceConfigTrimmer _instanceConfigTrimmer = new InstanceConfigTrimmer();

  /**
   * The following fields are considered as non-topology related.
   * HELIX_ENABLED,
   * HELIX_ENABLED_TIMESTAMP,
   * HELIX_DISABLED_PARTITION,
   * DELAY_REBALANCE_ENABLED,
   * MAX_CONCURRENT_TASK
   */
  private static final Map<FieldType, Set<String>> STATIC_TOPOLOGY_RELATED_FIELD_MAP = ImmutableMap
      .of(FieldType.SIMPLE_FIELD, ImmutableSet
              .of(InstanceConfigProperty.HELIX_HOST.name(),
                  InstanceConfigProperty.HELIX_PORT.name(),
                  InstanceConfigProperty.HELIX_ZONE_ID.name(),
                  InstanceConfigProperty.INSTANCE_WEIGHT.name(),
                  InstanceConfigProperty.DOMAIN.name()),
          FieldType.LIST_FIELD, ImmutableSet
              .of(InstanceConfigProperty.TAG_LIST.name()),
          FieldType.MAP_FIELD, ImmutableSet
              .of(InstanceConfigProperty.INSTANCE_CAPACITY_MAP.name()));

  private InstanceConfigTrimmer() {
  }

  @Override
  protected Map<FieldType, Set<String>> getNonTrimmableFields(InstanceConfig instanceConfig) {
    return STATIC_TOPOLOGY_RELATED_FIELD_MAP;
  }

  @Override
  public InstanceConfig trimProperty(InstanceConfig property) {
    return new InstanceConfig(doTrim(property));
  }

  public static InstanceConfigTrimmer getInstance() {
    return _instanceConfigTrimmer;
  }
}
