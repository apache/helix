package org.apache.helix.model;

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

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.helix.HelixConstants;

/**
 * A resource contains a set of partitions and its replicas are managed by a state model
 */
@Deprecated
public class Resource {
  private final String _resourceName;
  private final Map<String, Partition> _partitionMap;
  private String _stateModelDefRef;
  private String _stateModelFactoryName;
  private int _bucketSize = 0;
  private boolean _batchMessageMode = false;

  /**
   * Instantiate a resource by its name
   * @param resourceName the name of the resource that identifies it
   */
  public Resource(String resourceName) {
    this._resourceName = resourceName;
    this._partitionMap = new LinkedHashMap<String, Partition>();
  }

  /**
   * Get the state model definition managing this resource
   * @return a reference to the state model definition
   */
  public String getStateModelDefRef() {
    return _stateModelDefRef;
  }

  /**
   * Set the state model definition managing this resource
   * @param stateModelDefRef a reference to the state model definition
   */
  public void setStateModelDefRef(String stateModelDefRef) {
    _stateModelDefRef = stateModelDefRef;
  }

  /**
   * Set the state model factory for this resource
   * @param factoryName the name of the state model factory
   */
  public void setStateModelFactoryName(String factoryName) {
    if (factoryName == null) {
      _stateModelFactoryName = HelixConstants.DEFAULT_STATE_MODEL_FACTORY;
    } else {
      _stateModelFactoryName = factoryName;
    }
  }

  /**
   * Get the state model factory for this resource
   * @return the state model factory name
   */
  public String getStateModelFactoryname() {
    return _stateModelFactoryName;
  }

  /**
   * Get the resource name
   * @return the name of the resource, should be unique
   */
  public String getResourceName() {
    return _resourceName;
  }

  /**
   * Get the partitions of this resource
   * @return {@link Partition} objects
   */
  public Collection<Partition> getPartitions() {
    return _partitionMap.values();
  }

  /**
   * Add a partition to this resource
   * @param partitionName the name of the partition
   */
  public void addPartition(String partitionName) {
    _partitionMap.put(partitionName, new Partition(partitionName));
  }

  /**
   * Get a resource partition by name
   * @param partitionName partition name
   * @return the partition, or the name is not present
   */
  public Partition getPartition(String partitionName) {
    return _partitionMap.get(partitionName);
  }

  /**
   * Get the bucket size of this resource
   * @return the bucket size, or 0 if not specified
   */
  public int getBucketSize() {
    return _bucketSize;
  }

  /**
   * Set the bucket size of this resource
   * @param bucketSize the bucket size, or 0 to disable bucketizing
   */
  public void setBucketSize(int bucketSize) {
    _bucketSize = bucketSize;
  }

  /**
   * Set whether or not messages for this resource should be batch processed
   * @param mode true to batch process, false to disable batch processing
   */
  public void setBatchMessageMode(boolean mode) {
    _batchMessageMode = mode;
  }

  /**
   * Get the batch message processing mode
   * @return true if enabled, false if disabled
   */
  public boolean getBatchMessageMode() {
    return _batchMessageMode;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("resourceName:").append(_resourceName);
    sb.append(", stateModelDef:").append(_stateModelDefRef);
    sb.append(", bucketSize:").append(_bucketSize);
    sb.append(", partitionStateMap:").append(_partitionMap);

    return sb.toString();
  }
}
