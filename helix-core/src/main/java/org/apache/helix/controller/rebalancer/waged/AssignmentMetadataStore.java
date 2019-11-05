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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZNRecordJacksonSerializer;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ResourceAssignment;

/**
 * A placeholder before we have the real assignment metadata store.
 */
public class AssignmentMetadataStore {
  private static final String ASSIGNMENT_METADATA_KEY = "ASSIGNMENT_METADATA";
  private static final String BASELINE_TEMPLATE = "/%s/%s/BASELINE";
  private static final String BEST_POSSIBLE_TEMPLATE = "/%s/%s/BEST_POSSIBLE";
  private static final String BASELINE_KEY = "BASELINE";
  private static final String BEST_POSSIBLE_KEY = "BEST_POSSIBLE";
  private static final ZkSerializer SERIALIZER = new ZNRecordJacksonSerializer();

  private BucketDataAccessor _dataAccessor;
  private String _baselinePath;
  private String _bestPossiblePath;
  private Map<String, ResourceAssignment> _globalBaseline;
  private Map<String, ResourceAssignment> _bestPossibleAssignment;
  private boolean _useCache;

  AssignmentMetadataStore(String metadataStoreAddrs, String clusterName) {
    this(new ZkBucketDataAccessor(metadataStoreAddrs), clusterName, true);
  }

  protected AssignmentMetadataStore(BucketDataAccessor bucketDataAccessor, String clusterName) {
    this(bucketDataAccessor, clusterName, true);
  }

  protected AssignmentMetadataStore(BucketDataAccessor bucketDataAccessor, String clusterName,
      boolean useCache) {
    _dataAccessor = bucketDataAccessor;
    _baselinePath = String.format(BASELINE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
    _bestPossiblePath = String.format(BEST_POSSIBLE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
    _useCache = useCache;
  }

  public Map<String, ResourceAssignment> getBaseline() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (!_useCache || _globalBaseline == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_baselinePath, HelixProperty.class);
        _globalBaseline = splitAssignments(baseline);
      } catch (ZkNoNodeException ex) {
        // Metadata does not exist, so return an empty map
        _globalBaseline = Collections.emptyMap();
      }
    }
    return _globalBaseline;
  }

  public Map<String, ResourceAssignment> getBestPossibleAssignment() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (!_useCache || _bestPossibleAssignment == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_bestPossiblePath, HelixProperty.class);
        _bestPossibleAssignment = splitAssignments(baseline);
      } catch (ZkNoNodeException ex) {
        // Metadata does not exist, so return an empty map
        _bestPossibleAssignment = Collections.emptyMap();
      }
    }
    return _bestPossibleAssignment;
  }

  public void persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    // TODO: Make the write async?
    // If baseline hasn't changed, skip writing to metadata store
    if (_useCache && compareAssignments(_globalBaseline, globalBaseline)) {
      return;
    }
    // Persist to ZK
    HelixProperty combinedAssignments = combineAssignments(BASELINE_KEY, globalBaseline);
    try {
      _dataAccessor.compressedBucketWrite(_baselinePath, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException("Failed to persist baseline!", e);
    }

    // Update the in-memory reference
    _globalBaseline = globalBaseline;
  }

  public void persistBestPossibleAssignment(
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    // TODO: Make the write async?
    // If bestPossibleAssignment hasn't changed, skip writing to metadata store
    if (_useCache && compareAssignments(_bestPossibleAssignment, bestPossibleAssignment)) {
      return;
    }
    // Persist to ZK
    HelixProperty combinedAssignments =
        combineAssignments(BEST_POSSIBLE_KEY, bestPossibleAssignment);
    try {
      _dataAccessor.compressedBucketWrite(_bestPossiblePath, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException("Failed to persist BestPossibleAssignment!", e);
    }

    // Update the in-memory reference
    _bestPossibleAssignment = bestPossibleAssignment;
  }

  protected void finalize() {
    // To ensure all resources are released.
    close();
  }

  // Close to release all the resources.
  public void close() {
    _dataAccessor.disconnect();
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
    // Node that don't use Arrays.toString() for the record converting. The deserialize will fail.
    assignmentMap.forEach((resource, assignment) -> property.getRecord().setSimpleField(resource,
        new String(SERIALIZER.serialize(assignment.getRecord()))));
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
        .forEach((resource, assignmentStr) -> assignmentMap.put(resource,
            new ResourceAssignment((ZNRecord) SERIALIZER.deserialize(assignmentStr.getBytes()))));
    return assignmentMap;
  }

  /**
   * Returns whether two assignments are same.
   * @param oldAssignment
   * @param newAssignment
   * @return true if they are the same. False otherwise or oldAssignment is null
   */
  private boolean compareAssignments(Map<String, ResourceAssignment> oldAssignment,
      Map<String, ResourceAssignment> newAssignment) {
    // If oldAssignment is null, that means that we haven't read from/written to
    // the metadata store yet. In that case, we return false so that we write to metadata store.
    return oldAssignment != null && oldAssignment.equals(newAssignment);
  }
}
