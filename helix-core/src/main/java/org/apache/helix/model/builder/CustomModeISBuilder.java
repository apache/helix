package org.apache.helix.model.builder;

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
import java.util.TreeMap;

import org.apache.helix.api.State;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState.RebalanceMode;

/**
 * IdealState builder for CUSTOMIZED mode
 */
public class CustomModeISBuilder extends IdealStateBuilder {
  /**
   * Start building a CUSTOMIZED IdealState
   * @param resourceName the resource
   */
  public CustomModeISBuilder(String resourceName) {
    super(resourceName);
    setRebalancerMode(RebalanceMode.CUSTOMIZED);
  }

  /**
   * Start building a SEMI_AUTO IdealState
   * @param resourceId the resource
   */
  public CustomModeISBuilder(ResourceId resourceId) {
    this(resourceId.stringify());
  }

  /**
   * Add a sub-resource
   * @param partitionName partition to add
   * @return CustomModeISBuilder
   */
  public CustomModeISBuilder add(String partitionName) {
    if (_record.getMapField(partitionName) == null) {
      _record.setMapField(partitionName, new TreeMap<String, String>());
    }
    return this;
  }

  /**
   * Add a sub-resource
   * @param partitionId partition to add
   * @return CustomModeISBuilder
   */
  public CustomModeISBuilder add(PartitionId partitionId) {
    if (partitionId != null) {
      add(partitionId.stringify());
    }
    return this;
  }

  /**
   * add an instance->state assignment
   * @param partitionName partition to update
   * @param instanceName participant name
   * @param state state the replica should be in
   * @return CustomModeISBuilder
   */
  public CustomModeISBuilder assignInstanceAndState(String partitionName, String instanceName,
      String state) {
    add(partitionName);
    Map<String, String> partitionToInstanceStateMapping = _record.getMapField(partitionName);
    partitionToInstanceStateMapping.put(instanceName, state);
    return this;
  }

  /**
   * add an instance->state assignment
   * @param partitionId partition to update
   * @param participantId participant to assign to
   * @param state state the replica should be in
   * @return CustomModeISBuilder
   */
  public CustomModeISBuilder assignParticipantAndState(PartitionId partitionId,
      ParticipantId participantId, State state) {
    if (partitionId != null && participantId != null && state != null) {
      assignInstanceAndState(partitionId.stringify(), participantId.stringify(), state.toString());
    }
    return this;
  }

}
