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

import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.IdealState.RebalanceMode;

/**
 * IdealState builder for FULL_AUTO mode
 */
public class AutoRebalanceModeISBuilder extends IdealStateBuilder {
  /**
   * Start building a SEMI_AUTO IdealState
   * @param resourceName the resource
   */
  public AutoRebalanceModeISBuilder(String resourceName) {
    super(resourceName);
    setRebalancerMode(RebalanceMode.FULL_AUTO);
  }

  /**
   * Start building a SEMI_AUTO IdealState
   * @param resourceId the resource
   */
  public AutoRebalanceModeISBuilder(ResourceId resourceId) {
    this(resourceId.stringify());
  }

  /**
   * Add a partition, Helix will automatically assign the placement and state
   * for this partition at runtime.
   * @param partitionName the partition to add
   * @return AutoRebalanceModeISBuilder
   */
  public AutoRebalanceModeISBuilder add(String partitionName) {
    if (_record.getListField(partitionName) == null) {
      _record.setListField(partitionName, new ArrayList<String>());
    }
    return this;
  }

  /**
   * Add a partition, Helix will automatically assign the placement and state
   * for this partition at runtime.
   * @param partitionId the partition to add
   * @return AutoRebalanceModeISBuilder
   */
  public AutoRebalanceModeISBuilder add(PartitionId partitionId) {
    if (partitionId != null) {
      add(partitionId.stringify());
    }
    return this;
  }

}
