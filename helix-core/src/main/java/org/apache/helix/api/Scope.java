package org.apache.helix.api;

import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.Id;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;

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
 * Represents the scope of an object. For instance, a configuration can belong to a specific scope
 * of cluster, participant, partition, or resource, but not more than one of these at any time.
 */
public class Scope<T extends Id> {
  public enum ScopeType {
    CLUSTER,
    PARTICIPANT,
    PARTITION,
    RESOURCE
  }

  private final T _id;

  /**
   * Private: instantiate a scope with an id
   * @param id any object that extends Id
   */
  private Scope(T id) {
    _id = id;
  }

  /**
   * Get the scope that is tracked
   * @return The id of the scoped object
   */
  public T getScopedId() {
    return _id;
  }

  @Override
  public String toString() {
    return getType() + "{" + getScopedId() + "}";
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Scope) {
      return this.toString().equals(that.toString());
    }
    return false;
  }

  /**
   * Get the Helix entity type that this scope covers
   * @return scope type
   */
  public ScopeType getType() {
    Class<?> idClass = _id.getClass();
    if (ClusterId.class.equals(idClass)) {
      return ScopeType.CLUSTER;
    } else if (ParticipantId.class.equals(idClass)) {
      return ScopeType.PARTICIPANT;
    } else if (PartitionId.class.equals(idClass)) {
      return ScopeType.PARTITION;
    } else if (ResourceId.class.equals(idClass)) {
      return ScopeType.RESOURCE;
    } else {
      return null;
    }
  }

  /**
   * Get a cluster scope
   * @param clusterId the id of the cluster that is scoped
   * @return cluster scope
   */
  public static Scope<ClusterId> cluster(ClusterId clusterId) {
    return new Scope<ClusterId>(clusterId);
  }

  /**
   * Get a participant scope
   * @param participantId the id of the participant that is scoped
   * @return participant scope
   */
  public static Scope<ParticipantId> participant(ParticipantId participantId) {
    return new Scope<ParticipantId>(participantId);
  }

  /**
   * Get a partition scope
   * @param partitionId the id of the partition that is scoped
   * @return partition scope
   */
  public static Scope<PartitionId> partition(PartitionId partitionId) {
    return new Scope<PartitionId>(partitionId);
  }

  /**
   * Get a resource scope
   * @param resourceId the id of the resource that is scoped
   * @return resource scope
   */
  public static Scope<ResourceId> resource(ResourceId resourceId) {
    return new Scope<ResourceId>(resourceId);
  }
}
