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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.helix.controller.dataproviders.BaseControllerDataProvider;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A placeholder before we have the Cluster Data Detector implemented.
 *
 * @param <T> The cache class that can be handled by the detector.
 */
public class ClusterDataDetector<T extends BaseControllerDataProvider> {
  /**
   * All the cluster change type that may trigger a WAGED rebalancer re-calculation.
   */
  public enum ChangeType {
    BaselineAssignmentChange,
    InstanceConfigChange,
    ClusterConfigChange,
    ResourceConfigChange,
    ResourceIdealStatesChange,
    InstanceStateChange,
    OtherChange
  }

  private Map<ChangeType, Set<String>> _currentChanges =
      Collections.singletonMap(ChangeType.ClusterConfigChange, Collections.emptySet());

  public void updateClusterStatus(T cache) {
  }

  /**
   * Returns all change types detected during the ClusterDetection stage.
   */
  public Set<ChangeType> getChangeTypes() {
    return _currentChanges.keySet();
  }

  /**
   * Returns a set of the names of components that changed based on the given change type.
   */
  public Set<String> getChangesBasedOnType(ChangeType changeType) {
    return _currentChanges.get(changeType);
  }

  /**
   * Return a map of the change details <type, change details>.
   */
  public Map<ChangeType, Set<String>> getAllChanges() {
    return _currentChanges;
  }
}
