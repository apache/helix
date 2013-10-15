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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState.RebalanceMode;

/**
 * IdealState builder for SEMI_AUTO mode
 */
public class AutoModeISBuilder extends IdealStateBuilder {
  /**
   * Start building a SEMI_AUTO IdealState
   * @param resourceName the resource
   */
  public AutoModeISBuilder(String resourceName) {
    super(resourceName);
    setRebalancerMode(RebalanceMode.SEMI_AUTO);
  }

  /**
   * Start building a SEMI_AUTO IdealState
   * @param resourceId the resource
   */
  public AutoModeISBuilder(ResourceId resourceId) {
    this(resourceId.stringify());
  }

  /**
   * Add a partition; Helix will assign replicas of the partition according to preference lists
   * @param partitionName the name of the new partition
   * @return AutoModeISBuilder
   */
  public AutoModeISBuilder add(String partitionName) {
    if (_record.getListField(partitionName) == null) {
      _record.setListField(partitionName, new ArrayList<String>());
    }
    return this;
  }

  /**
   * Add a partition; Helix will assign replicas of the partition according to preference lists
   * @param partitionId the id of the new partition
   * @return AutoModeISBuilder
   */
  public AutoModeISBuilder add(PartitionId partitionId) {
    if (partitionId != null) {
      add(partitionId.stringify());
    }
    return this;
  }

  /**
   * Define where replicas of a partition should live
   * @param partitionName the partition
   * @param instanceNames ordered list of participant names
   * @return AutoModeISBuilder
   */
  public AutoModeISBuilder assignPreferenceList(String partitionName, String... instanceNames) {
    add(partitionName);
    _record.getListField(partitionName).addAll(Arrays.asList(instanceNames));
    return this;
  }

  /**
   * Define where replicas of a partition should live
   * @param partitionId the partition
   * @param participantIds ordered list of participant ids
   * @return AutoModeISBuilder
   */
  public AutoModeISBuilder assignPreferenceList(PartitionId partitionId,
      ParticipantId... participantIds) {
    if (partitionId != null) {
      String[] participantNames = new String[participantIds.length];
      for (int i = 0; i < participantIds.length; i++) {
        participantNames[i] = participantIds[i].stringify();
      }
      assignPreferenceList(partitionId.stringify(), participantNames);
    }
    return this;
  }

}
