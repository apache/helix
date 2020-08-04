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

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.BucketDataAccessor;
import org.apache.helix.HelixException;
import org.apache.helix.HelixProperty;
import org.apache.helix.manager.zk.ZkBucketDataAccessor;
import org.apache.helix.model.ResourceAssignment;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.datamodel.serializer.ZNRecordJacksonSerializer;
import org.apache.helix.zookeeper.zkclient.exception.ZkNoNodeException;
import org.apache.helix.zookeeper.zkclient.serialize.ZkSerializer;

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
  protected Map<String, ResourceAssignment> _globalBaseline;
  protected Map<String, ResourceAssignment> _bestPossibleAssignment;

  AssignmentMetadataStore(String metadataStoreAddrs, String clusterName) {
    this(new ZkBucketDataAccessor(metadataStoreAddrs), clusterName);
  }

  protected AssignmentMetadataStore(BucketDataAccessor bucketDataAccessor, String clusterName) {
    _dataAccessor = bucketDataAccessor;
    _baselinePath = String.format(BASELINE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
    _bestPossiblePath = String.format(BEST_POSSIBLE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
  }

  public synchronized Map<String, ResourceAssignment> getBaseline() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_globalBaseline == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_baselinePath, HelixProperty.class);
        _globalBaseline = splitAssignments(baseline);
      } catch (ZkNoNodeException ex) {
        // Metadata does not exist, so return an empty map
        _globalBaseline = new HashMap<>();
      }
    }
    return _globalBaseline;
  }

  public synchronized Map<String, ResourceAssignment> getBestPossibleAssignment() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_bestPossibleAssignment == null) {
      try {
        HelixProperty baseline =
            _dataAccessor.compressedBucketRead(_bestPossiblePath, HelixProperty.class);
        _bestPossibleAssignment = splitAssignments(baseline);
      } catch (ZkNoNodeException ex) {
        // Metadata does not exist, so return an empty map
        _bestPossibleAssignment = new HashMap<>();
      }
    }
    return _bestPossibleAssignment;
  }

  /**
   * @return true if a new baseline was persisted.
   * @throws HelixException if the method failed to persist the baseline.
   */
  public synchronized boolean persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    return persistAssignment(globalBaseline, getBaseline(), _baselinePath, BASELINE_KEY);
  }

  /**
   * @return true if a new best possible assignment was persisted.
   * @throws HelixException if the method failed to persist the baseline.
   */
  public synchronized boolean persistBestPossibleAssignment(
      Map<String, ResourceAssignment> bestPossibleAssignment) {
    return persistAssignment(bestPossibleAssignment, getBestPossibleAssignment(), _bestPossiblePath,
        BEST_POSSIBLE_KEY);
  }

  public synchronized void clearAssignmentMetadata() {
    persistAssignment(Collections.emptyMap(), getBaseline(), _baselinePath, BASELINE_KEY);
    persistAssignment(Collections.emptyMap(), getBestPossibleAssignment(), _bestPossiblePath,
        BEST_POSSIBLE_KEY);
  }

  /**
   * @param newAssignment
   * @param cachedAssignment
   * @param path the path of the assignment record
   * @param key  the key of the assignment in the record
   * @return true if a new assignment was persisted.
   */
  // TODO: Enhance the return value so it is more intuitive to understand when the persist fails and
  // TODO: when it is skipped.
  private boolean persistAssignment(Map<String, ResourceAssignment> newAssignment,
      Map<String, ResourceAssignment> cachedAssignment, String path,
      String key) {
    // TODO: Make the write async?
    // If the assignment hasn't changed, skip writing to metadata store
    if (compareAssignments(cachedAssignment, newAssignment)) {
      return false;
    }
    // Persist to ZK
    HelixProperty combinedAssignments = combineAssignments(key, newAssignment);
    try {
      _dataAccessor.compressedBucketWrite(path, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException(
          String.format("Failed to persist %s assignment to path %s", key, path), e);
    }

    // Update the in-memory reference
    cachedAssignment.clear();
    cachedAssignment.putAll(newAssignment);
    return true;
  }

  protected synchronized void reset() {
    if (_bestPossibleAssignment != null) {
      _bestPossibleAssignment.clear();
      _bestPossibleAssignment = null;
    }
    if (_globalBaseline != null) {
      _globalBaseline.clear();
      _globalBaseline = null;
    }
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
  protected boolean compareAssignments(Map<String, ResourceAssignment> oldAssignment,
      Map<String, ResourceAssignment> newAssignment) {
    // If oldAssignment is null, that means that we haven't read from/written to
    // the metadata store yet. In that case, we return false so that we write to metadata store.
    return oldAssignment != null && oldAssignment.equals(newAssignment);
  }
}
