package org.apache.helix;

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

/*
 * Identifying constants of the components in a Helix-managed cluster
 */
public interface HelixConstants {
  // TODO: ChangeType and PropertyType are duplicated, consider unifying
  enum ChangeType {
    // @formatter:off
    IDEAL_STATE,
    CONFIG,
    INSTANCE_CONFIG,
    RESOURCE_CONFIG,
    CLUSTER_CONFIG,
    LIVE_INSTANCE,
    CURRENT_STATE,
    MESSAGE,
    EXTERNAL_VIEW,
    TARGET_EXTERNAL_VIEW,
    CONTROLLER,
    MESSAGES_CONTROLLER,
    HEALTH
    // @formatter:on
  }

  /**
   * Use IdealState.PreferentListToken instead.
   */
  @Deprecated
  enum StateModelToken {
    ANY_LIVEINSTANCE
  }

  /**
   * Please use ClusterConfig instead
   */
  @Deprecated
  enum ClusterConfigType {
    HELIX_DISABLE_PIPELINE_TRIGGERS,
    PERSIST_BEST_POSSIBLE_ASSIGNMENT
  }

  String DEFAULT_STATE_MODEL_FACTORY = "DEFAULT";
}
