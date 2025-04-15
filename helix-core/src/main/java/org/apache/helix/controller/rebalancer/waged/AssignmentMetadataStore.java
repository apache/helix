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

import java.util.Objects;
import java.util.stream.Collectors;
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

  private final BucketDataAccessor _dataAccessor;
  private final String _baselinePath;
  private final String _bestPossiblePath;
  // volatile for double-checked locking
  protected volatile Map<String, ResourceAssignment> _globalBaseline = null;
  protected volatile Map<String, ResourceAssignment> _bestPossibleAssignment = null;
  protected volatile int _bestPossibleVersion = 0;
  protected volatile int _lastPersistedBestPossibleVersion = 0;

  AssignmentMetadataStore(String metadataStoreAddrs, String clusterName) {
    this(new ZkBucketDataAccessor(metadataStoreAddrs), clusterName);
  }

  protected AssignmentMetadataStore(BucketDataAccessor bucketDataAccessor, String clusterName) {
    _dataAccessor = bucketDataAccessor;
    _baselinePath = String.format(BASELINE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
    _bestPossiblePath = String.format(BEST_POSSIBLE_TEMPLATE, clusterName, ASSIGNMENT_METADATA_KEY);
  }

  /**
   * @return a deep copy of the best possible assignment that is safe for modification.
   */
  public Map<String, ResourceAssignment> getBaseline() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_globalBaseline == null) {
      // double-checked locking
      synchronized (this) {
        if (_globalBaseline == null) {
          _globalBaseline = fetchAssignmentOrDefault(_baselinePath);
        }
      }
    }
    return _globalBaseline.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            entry -> new ResourceAssignment(entry.getValue().getRecord())));
  }

  /**
   * Check to see if the latest persisted version of best possible assignment in the cache has been
   * persisted to metadata store.
   *
   * @return true if the latest version has been persisted, false otherwise.
   */
  protected boolean hasPersistedLatestBestPossibleAssignment() {
    return _lastPersistedBestPossibleVersion == _bestPossibleVersion;
  }

  /**
   * @return a deep copy of the best possible assignment that is safe for modification.
   */
  public Map<String, ResourceAssignment> getBestPossibleAssignment() {
    // Return the in-memory baseline. If null, read from ZK. This is to minimize reads from ZK
    if (_bestPossibleAssignment == null) {
      // double-checked locking
      synchronized (this) {
        if (_bestPossibleAssignment == null) {
          _bestPossibleAssignment = fetchAssignmentOrDefault(_bestPossiblePath);
        }
      }
    }
    // Return defensive copy so that the in-memory assignment is not modified by callers
    return _bestPossibleAssignment.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            entry -> new ResourceAssignment(entry.getValue().getRecord())));
  }

  private Map<String, ResourceAssignment> fetchAssignmentOrDefault(String path) {
    try {
      HelixProperty assignment = _dataAccessor.compressedBucketRead(path, HelixProperty.class);
      return splitAssignments(assignment);
    } catch (ZkNoNodeException ex) {
      // Metadata does not exist, so return an empty map
      return new HashMap<>();
    }
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
      throw new HelixException(String.format("Failed to persist %s assignment to path %s", key, path), e);
    }
  }

  /**
   * Persist a new baseline assignment to metadata store first, then to memory
   * @param globalBaseline
   */
  public synchronized void persistBaseline(Map<String, ResourceAssignment> globalBaseline) {
    // Create defensive copy so that the in-memory assignment is not modified after it is persisted
    Map<String, ResourceAssignment> baselineCopy = globalBaseline.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            entry -> new ResourceAssignment(entry.getValue().getRecord())));
    // write to metadata store
    persistAssignmentToMetadataStore(baselineCopy, _baselinePath, BASELINE_KEY);
    // write to memory
    _globalBaseline = baselineCopy;
  }

  /**
   * Persist a new best possible assignment to metadata store first, then to memory.
   * Increment best possible version by 1 - this is a high priority in-memory write.
   * @param bestPossibleAssignment
   */
  public synchronized void persistBestPossibleAssignment(Map<String, ResourceAssignment> bestPossibleAssignment) {
    // Create defensive copy so that the in-memory assignment is not modified after it is persisted
    Map<String, ResourceAssignment> bestPossibleAssignmentCopy = bestPossibleAssignment.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            entry -> new ResourceAssignment(entry.getValue().getRecord())));
    // write to metadata store
    persistAssignmentToMetadataStore(bestPossibleAssignmentCopy, _bestPossiblePath, BEST_POSSIBLE_KEY);
    // write to memory
    _bestPossibleAssignment = bestPossibleAssignmentCopy;
    _bestPossibleVersion++;
    _lastPersistedBestPossibleVersion = _bestPossibleVersion;
  }

  /**
   * Attempts to persist Best Possible Assignment in memory from an asynchronous thread.
   * Persist only happens when the provided version is not stale - this is a low priority in-memory write.
   * @param bestPossibleAssignment - new assignment to be persisted
   * @param newVersion - attempted new version to write. This version is obtained earlier from getBestPossibleVersion()
   * @return true if the attempt succeeded, false otherwise.
   */
  public synchronized boolean asyncUpdateBestPossibleAssignmentCache(
      Map<String, ResourceAssignment> bestPossibleAssignment, int newVersion) {
    Map<String, ResourceAssignment> bestPossibleAssignmentCopy = bestPossibleAssignment.entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey,
            entry -> new ResourceAssignment(entry.getValue().getRecord())));
    // Check if the version is stale by this point
    if (newVersion > _bestPossibleVersion) {
      _bestPossibleAssignment = bestPossibleAssignmentCopy;
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
    _globalBaseline = new HashMap<>();
    _bestPossibleAssignment = new HashMap<>();
  }

  protected synchronized void reset() {
    _bestPossibleAssignment = null;
    _globalBaseline = null;
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

  protected boolean isBaselineChanged(Map<String, ResourceAssignment> newBaseline) {
    return !Objects.equals(_globalBaseline, newBaseline);
  }

  protected boolean isBestPossibleChanged(Map<String, ResourceAssignment> newBestPossible) {
    return !Objects.equals(_bestPossibleAssignment, newBestPossible);
  }
}
