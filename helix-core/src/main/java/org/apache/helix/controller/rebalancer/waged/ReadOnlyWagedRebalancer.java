package org.apache.helix.controller.rebalancer.waged;

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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.helix.HelixRebalanceException;
import org.apache.helix.controller.dataproviders.ResourceControllerDataProvider;
import org.apache.helix.controller.rebalancer.waged.constraints.ConstraintBasedAlgorithmFactory;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ClusterConfig;
import org.apache.helix.model.ResourceAssignment;


/**
 * This rebalancer is a version of WagedRebalancer that only reads the existing assignment metadata
 * to compute the best possible assignment but never writes back the resulting assignment metadata
 * from global or partial rebalance. It does so by using a modified version of
 * AssignmentMetadataStore, ReadOnlyAssignmentMetadataStore.
 *
 * This class is to be used in the cluster verifiers, tests, or util methods.
 */
public class ReadOnlyWagedRebalancer extends WagedRebalancer {
  public ReadOnlyWagedRebalancer(ZkBucketDataAccessor zkBucketDataAccessor, String clusterName,
      Map<ClusterConfig.GlobalRebalancePreferenceKey, Integer> preferences) {
    super(new ReadOnlyAssignmentMetadataStore(zkBucketDataAccessor, clusterName),
        ConstraintBasedAlgorithmFactory.getInstance(preferences), Optional.empty());
  }

  @Override
  protected List<HelixRebalanceException.Type> failureTypesToPropagate() {
    // Also propagate FAILED_TO_CALCULATE for ReadOnlyWagedRebalancer
    List<HelixRebalanceException.Type> failureTypes =
        new ArrayList<>(super.failureTypesToPropagate());
    failureTypes.add(HelixRebalanceException.Type.FAILED_TO_CALCULATE);
    return failureTypes;
  }

  public void updateChangeDetectorSnapshots(ResourceControllerDataProvider dataProvider) {
    getChangeDetector().updateSnapshots(dataProvider);
  }

  private static class ReadOnlyAssignmentMetadataStore extends AssignmentMetadataStore {

    ReadOnlyAssignmentMetadataStore(ZkBucketDataAccessor zkBucketDataAccessor, String clusterName) {
      super(zkBucketDataAccessor, clusterName);
    }

    @Override
    public boolean persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
      // If baseline hasn't changed, skip updating the metadata store
      if (compareAssignments(_globalBaseline, globalBaseline)) {
        return false;
      }
      // Update the in-memory reference only
      _globalBaseline = globalBaseline;
      return true;
    }

    @Override
    public boolean persistBestPossibleAssignment(
        Map<String, ResourceAssignment> bestPossibleAssignment) {
      // If bestPossibleAssignment hasn't changed, skip updating the metadata store
      if (compareAssignments(_bestPossibleAssignment, bestPossibleAssignment)) {
        return false;
      }
      // Update the in-memory reference only
      _bestPossibleAssignment = bestPossibleAssignment;
      return true;
    }
  }
}
