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
import org.apache.helix.model.IdealState;

import static org.apache.helix.model.IdealState.IdealStateProperty;

/**
 * A singleton HelixProperty Trimmer for IdealState to remove the non-cluster-topology-related
 * fields.
 */
public class IdealStateTrimmer extends HelixPropertyTrimmer<IdealState> {
  private static final IdealStateTrimmer IDEAL_STATE_TRIMMER = new IdealStateTrimmer();

  /**
   * The following fields are considered as non-topology related.
   * REBALANCE_DELAY,
   * DELAY_REBALANCE_DISABLED,
   * REBALANCE_TIMER_PERIOD,
   * GROUP_ROUTING_ENABLED,
   * EXTERNAL_VIEW_DISABLED,
   * HELIX_ENABLED,
   * ResourceConfigProperty.DELAY_REBALANCE_ENABLED
   */
  private static final Map<FieldType, Set<String>> STATIC_TOPOLOGY_RELATED_FIELD_MAP = ImmutableMap
      .of(FieldType.SIMPLE_FIELD, ImmutableSet.of(
          IdealStateProperty.NUM_PARTITIONS.name(),
          IdealStateProperty.STATE_MODEL_DEF_REF.name(),
          IdealStateProperty.STATE_MODEL_FACTORY_NAME.name(),
          IdealStateProperty.REPLICAS.name(),
          IdealStateProperty.MIN_ACTIVE_REPLICAS.name(),
          IdealStateProperty.IDEAL_STATE_MODE.name(),
          IdealStateProperty.REBALANCE_MODE.name(),
          IdealStateProperty.REBALANCER_CLASS_NAME.name(),
          IdealStateProperty.REBALANCE_STRATEGY.name(),
          IdealStateProperty.MAX_PARTITIONS_PER_INSTANCE.name(),
          IdealStateProperty.INSTANCE_GROUP_TAG.name(),
          IdealStateProperty.RESOURCE_GROUP_NAME.name(),
          IdealStateProperty.RESOURCE_TYPE.name()));

  private IdealStateTrimmer() {
  }

  @Override
  protected Map<FieldType, Set<String>> getNonTrimmableFields(IdealState idealState) {
    Map<FieldType, Set<String>> nonTrimmableFields =
        new HashMap<>(STATIC_TOPOLOGY_RELATED_FIELD_MAP);
    // Also don't trim the application defined assignment in the list and map fields.
    // They are fixed and considered as part of the cluster topology.
    switch (idealState.getRebalanceMode()) {
    case CUSTOMIZED:
      // For CUSTOMZIED resources, map fields are user configured partition state assignment. So
      // they are not trimmable.
      nonTrimmableFields.put(FieldType.MAP_FIELD, idealState.getRecord().getMapFields().keySet());
      break;
    case SEMI_AUTO:
      // For SEMI_AUTO resources, list fields are user configured partition placement. So it is not
      // trimmable.
      nonTrimmableFields.put(FieldType.LIST_FIELD, idealState.getRecord().getListFields().keySet());
      break;
    case FULL_AUTO:
      // For FULL_AUTO resources, both map fields and list fields are trimmable since they are
      // re-computed and updated in every controller rebalance pipelines.
    default:
      break;
    }
    return nonTrimmableFields;
  }

  @Override
  public IdealState trimProperty(IdealState property) {
    return new IdealState(doTrim(property));
  }

  public static IdealStateTrimmer getInstance() {
    return IDEAL_STATE_TRIMMER;
  }
}
