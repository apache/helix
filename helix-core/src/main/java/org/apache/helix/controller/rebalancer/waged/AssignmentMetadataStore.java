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
  protected int _bestPossibleVersion = 0;

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
   * @param newAssignment
   * @param path the path of the assignment record
   * @param key  the key of the assignment in the record
   * @throws HelixException if the method failed to persist the baseline.
   */
  private void persistAssignmentToMetadataStore(Map<String, ResourceAssignment> newAssignment, String path, String key)
      throws HelixException {
    // TODO: Make the write async?
    // Persist to ZK
    HelixProperty combinedAssignments = combineAssignments(key, newAssignment);
    try {
      _dataAccessor.compressedBucketWrite(path, combinedAssignments);
    } catch (IOException e) {
      // TODO: Improve failure handling
      throw new HelixException(String.format("Failed to persist %s assignment to path %s", key, path), e);
    }
  }

  /**
   * Persist a new baseline assignment to metadata store first, then to memory
   * @param globalBaseline
   */
  public synchronized void persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    // write to metadata store
    persistAssignmentToMetadataStore(globalBaseline, _baselinePath, BASELINE_KEY);
    // write to memory
    getBaseline().clear();
    getBaseline().putAll(globalBaseline);
  }

  /**
   * Persist a new best possible assignment to metadata store first, then to memory.
   * Increment best possible version by 1 - this is a high priority in-memory write.
   * @param bestPossibleAssignment
   */
  public synchronized void persistBestPossibleAssignment(Map<String, ResourceAssignment> bestPossibleAssignment) {
    // write to metadata store
    persistAssignmentToMetadataStore(bestPossibleAssignment, _bestPossiblePath, BEST_POSSIBLE_KEY);
    // write to memory
    getBestPossibleAssignment().clear();
    getBestPossibleAssignment().putAll(bestPossibleAssignment);
    _bestPossibleVersion++;
  }

  /**
   * Attempts to persist Best Possible Assignment in memory from an asynchronous thread.
   * Persist only happens when the provided version is not stale - this is a low priority in-memory write.
   * @param bestPossibleAssignment - new assignment to be persisted
   * @param newVersion - attempted new version to write. This version is obtained earlier from getBestPossibleVersion()
   * @return true if the attempt succeeded, false otherwise.
   */
  public synchronized boolean asyncPersistBestPossibleAssignmentInMemory(
      Map<String, ResourceAssignment> bestPossibleAssignment, int newVersion) {
    // Check if the version is stale by this point
    if (newVersion > _bestPossibleVersion) {
      getBestPossibleAssignment().clear();
      getBestPossibleAssignment().putAll(bestPossibleAssignment);
      _bestPossibleVersion = newVersion;
      return true;
    }

    return false;
  }

  public int getBestPossibleVersion() {
    return _bestPossibleVersion;
  }

  public synchronized void clearAssignmentMetadata() {
    persistAssignmentToMetadataStore(Collections.emptyMap(), _baselinePath, BASELINE_KEY);
    persistAssignmentToMetadataStore(Collections.emptyMap(), _bestPossiblePath, BEST_POSSIBLE_KEY);
    getBaseline().clear();
    getBestPossibleAssignment().clear();
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
  private boolean compareAssignments(Map<String, ResourceAssignment> oldAssignment,
      Map<String, ResourceAssignment> newAssignment) {
    // If oldAssignment is null, that means that we haven't read from/written to
    // the metadata store yet. In that case, we return false so that we write to metadata store.
    return oldAssignment != null && oldAssignment.equals(newAssignment);
  }

  protected boolean compareBaseline(Map<String, ResourceAssignment> newBaseline) {
    return compareAssignments(getBaseline(), newBaseline);
  }

  protected boolean compareBestPossibleAssignment(Map<String, ResourceAssignment> newBaseline) {
    return compareAssignments(getBestPossibleAssignment(), newBaseline);
  }
}
