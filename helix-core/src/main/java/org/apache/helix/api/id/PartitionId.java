package org.apache.helix.api.id;

import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

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

public class PartitionId extends Id {
  @JsonProperty("resourceId")
  private final ResourceId _resourceId;
  @JsonProperty("partitionName")
  private final String _partitionName;

  /**
   * Instantiate for a resource and suffix
   * @param resourceId resource that the partition belongs to
   * @param partitionName name of the partition relative to the resource
   */
  @JsonCreator
  public PartitionId(@JsonProperty("resourceId") ResourceId resourceId,
      @JsonProperty("partitionName") String partitionName) {
    _resourceId = resourceId;
    _partitionName = partitionName;
  }

  /**
   * Get the id of the resource containing this partition
   * @return ResourceId
   */
  public ResourceId getResourceId() {
    return _resourceId;
  }

  @Override
  public String stringify() {
    // check in case the partition name is instantiated incorrectly
    if (_resourceId.stringify().equals(_partitionName)) {
      return _partitionName;
    }
    return String.format("%s_%s", _resourceId, _partitionName);
  }

  /**
   * @param partitionName
   * @return
   */
  public static String stripResourceId(String partitionName) {
    if (partitionName == null || !partitionName.contains("_")) {
      return partitionName;
    }
    return partitionName.substring(partitionName.lastIndexOf("_") + 1);
  }

  /**
   * @param partitionName
   * @return
   */
  public static ResourceId extractResourceId(String partitionName) {
    if (partitionName == null || !partitionName.contains("_")) {
      return ResourceId.from(partitionName);
    }
    return ResourceId.from(partitionName.substring(0, partitionName.lastIndexOf("_")));
  }

  /**
   * Get a concrete partition id
   * @param partitionId string partition identifier
   * @return PartitionId
   */
  public static PartitionId from(String partitionId) {
    if (partitionId == null) {
      return null;
    }
    return new PartitionId(extractResourceId(partitionId), stripResourceId(partitionId));
  }

  /**
   * Same as {@link PartitionId#from(String)}.
   * @param partitionId string partition identifier
   * @return PartitionId
   */
  public static PartitionId valueOf(String partitionId) {
    return from(partitionId);
  }

  /**
   * Get a concrete partition id
   * @param resourceId resource identifier
   * @param partitionSuffix partition identifier relative to a resource
   * @return PartitionId
   */
  public static PartitionId from(ResourceId resourceId, String partitionSuffix) {
    return new PartitionId(resourceId, partitionSuffix);
  }
}
