package org.apache.helix.examples.rebalancer.simulator;

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

import java.util.Map;

import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.ResourceConfig;

/**
 * Overwrites the cluster metadata for the simulation purposes.
 */
public interface MetadataOverwrite {
  /**
   * @param clusterConfig the existing cluster config.
   * @return the new cluster config.
   */
  ClusterConfig updateClusterConfig(ClusterConfig clusterConfig);

  /**
   * @param resourceConfigMap the existing resource config map.
   * @param idealStateMap     the existing ideal state map.
   * @return the new resource config map that overwrites the existing ones. Note the if one config
   * item does not appear in the new map, it will be removed from metadata.
   */
  Map<String, ResourceConfig> updateResourceConfigs(Map<String, ResourceConfig> resourceConfigMap,
      Map<String, IdealState> idealStateMap);

  /**
   * @param idealStateMap the existing ideal state map.
   * @return the new idea state map that overwrites the existing ones. Note the if one config
   * item does not appear in the new map, it will be removed from metadata.
   */
  Map<String, IdealState> updateIdealStates(Map<String, IdealState> idealStateMap);

  /**
   * @param instanceConfigMap the existing instance config map.
   * @return the new instance config map that overwrites the existing ones. Note the if one config
   * item does not appear in the new map, it will be removed from metadata.
   */
  Map<String, InstanceConfig> updateInstanceConfig(ClusterConfig clusterConfig,
      Map<String, InstanceConfig> instanceConfigMap);
}
