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
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Objects;


/**
 * A POJO class containing {@link Partition} with missing replicas.
 */
public class PartitionWithReplicaCount {

  private final Partition _partition;
  private final int _minActiveReplica;
  private final int _numReplica;

  public PartitionWithReplicaCount(Partition partition, int minActiveReplica, int numReplica) {
    _partition = partition;
    _minActiveReplica = Math.min(minActiveReplica, numReplica);
    _numReplica = numReplica;
  }

  public Partition getPartition() {
    return _partition;
  }

  public int getMinActiveReplica() {
    return _minActiveReplica;
  }

  public int getNumReplica() {
    return _numReplica;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionWithReplicaCount that = (PartitionWithReplicaCount) o;
    return _minActiveReplica == that._minActiveReplica && _numReplica == that._numReplica
        && _partition.equals(that._partition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partition, _minActiveReplica, _numReplica);
  }
}
