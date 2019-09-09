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

import java.io.IOException;
import java.util.Arrays;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordJacksonSerializer;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ResourceAssignment;

import java.util.HashMap;
import java.util.Map;

/**
 * A placeholder before we have the real assignment metadata store.
 */
public class AssignmentMetadataStore {
  private static final String ASSIGNMENT_METADATA_KEY = "ASSIGNMENT_METADATA";
  private static final String BASELINE_TEMPLATE = "/%s/%s/BASELINE";
  private static final String BEST_POSSIBLE_TEMPLATE = "/%s/%s/BEST_POSSIBLE";
  private static final ZkSerializer SERIALIZER = new ZNRecordJacksonSerializer();

  private BucketDataAccessor _dataAccessor;
  private String _baselinePath;
  private String _bestPossiblePath;
  private Map<String, ResourceAssignment> _globalBaseline;
  private Map<String, ResourceAssignment> _bestPossibleAssignment;

  public AssignmentMetadataStore(HelixManager helixManager) {
    _dataAccessor = new ZkBucketDataAccessor(helixManager.getMetadataStoreConnectionString());
    _baselinePath =
        String.format(BASELINE_TEMPLATE, helixManager.getClusterName(), ASSIGNMENT_METADATA_KEY);
    _bestPossiblePath = String.format(BEST_POSSIBLE_TEMPLATE, helixManager.getClusterName(),
        ASSIGNMENT_METADATA_KEY);
  }

  public Map<String, ResourceAssignment> getBaseline() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_globalBaseline == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_baselinePath, HelixProperty.class);
        _globalBaseline = splitAssignments(baseline);
      } catch (HelixException e) {
        // Metadata does not exist, so return an empty map
        _globalBaseline = new HashMap<>();
      }
    }
    return _globalBaseline;
  }

  public Map<String, ResourceAssignment> getBestPossibleAssignment() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_bestPossibleAssignment == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_bestPossiblePath, HelixProperty.class);
        _bestPossibleAssignment = splitAssignments(baseline);
      } catch (HelixException e) {
        // Metadata does not exist, so return an empty map
        _bestPossibleAssignment = new HashMap<>();
      }
    }
    return _bestPossibleAssignment;
  }

  public void persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    // Update the in-memory reference
    _globalBaseline = globalBaseline;

    // TODO: Make the write async?
    // Persist to ZK
    HelixProperty combinedAssignments = combineAssignments(BASELINE_KEY, globalBaseline);
    try {
      _dataAccessor.compressedBucketWrite(_baselinePath, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException("Failed to persist baseline!", e);
    }
  }

  public void persistBestPossibleAssignment(
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    // Update the in-memory reference
    _bestPossibleAssignment.putAll(bestPossibleAssignment);

    // TODO: Make the write async?
    // Persist to ZK asynchronously
    HelixProperty combinedAssignments =
        combineAssignments(BEST_POSSIBLE_KEY, bestPossibleAssignment);
    try {
      _dataAccessor.compressedBucketWrite(_bestPossiblePath, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException("Failed to persist baseline!", e);
    }
  }

  /**
   * Produces one HelixProperty that contains all assignment data.
   * @param name
   * @param assignmentMap
   * @return
   */
  private HelixProperty combineAssignments(String name,
      Map<String, ResourceAssignment> assignmentMap) {
    HelixProperty property = new HelixProperty(name);
    // Add each resource's assignment as a simple field in one ZNRecord
    assignmentMap.forEach((resource, assignment) -> property.getRecord().setSimpleField(resource,
        Arrays.toString(SERIALIZER.serialize(assignment.getRecord()))));
    return property;
  }

  /**
   * Returns a Map of (ResourceName, ResourceAssignment) pairs.
   * @param property
   * @return
   */
  private Map<String, ResourceAssignment> splitAssignments(HelixProperty property) {
    Map<String, ResourceAssignment> assignmentMap = new HashMap<>();
    // Convert each resource's assignment String into a ResourceAssignment object and put it in a
    // map
    property.getRecord().getSimpleFields()
        .forEach((resource, assignment) -> assignmentMap.put(resource,
            new ResourceAssignment((ZNRecord) SERIALIZER.deserialize(assignment.getBytes()))));
    return assignmentMap;
  }
}
