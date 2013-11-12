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

/**
 * A distinct partition of a resource
 * Deprecated. Use {@link org.apache.helix.api.Partition}
 */
@Deprecated
public class Partition {
  private final String _partitionName;

  /**
   * Get the name of the partition, unique for the resource
   * @return partition name
   */
  public String getPartitionName() {
    return _partitionName;
  }

  /**
   * Instantiate with a partition name
   * @param partitionName unique partition name within a resource
   */
  public Partition(String partitionName) {
    this._partitionName = partitionName;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Partition)) {
      return false;
    }

    Partition that = (Partition) obj;
    return _partitionName.equals(that.getPartitionName());
  }

  @Override
  public int hashCode() {
    return _partitionName.hashCode();
  }

  @Override
  public String toString() {
    return _partitionName;
  }
}
