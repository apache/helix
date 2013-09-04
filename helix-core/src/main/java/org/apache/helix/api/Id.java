package org.apache.helix.api;

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
 *
 */
public abstract class Id implements Comparable<Id> {
  public abstract String stringify();

  @Override
  public String toString() {
    return stringify();
  }

  @Override
  public boolean equals(Object that) {
    if (that instanceof Id) {
      return this.stringify().equals(((Id) that).stringify());
    } else if (that instanceof String) {
      return this.stringify().equals(that);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return this.stringify().hashCode();
  }

  @Override
  public int compareTo(Id that) {
    if (that instanceof Id) {
      return this.stringify().compareTo(that.stringify());
    }
    return -1;
  }

  /**
   * Get a concrete cluster id for a string name
   * @param clusterId string cluter identifier
   * @return ClusterId
   */
  public static ClusterId cluster(String clusterId) {
    if (clusterId == null) {
      return null;
    }
    return new ClusterId(clusterId);
  }

  /**
   * Get a concrete resource id for a string name
   * @param resourceId string resource identifier
   * @return ResourceId
   */
  public static ResourceId resource(String resourceId) {
    if (resourceId == null) {
      return null;
    }
    return new ResourceId(resourceId);
  }

  /**
   * Get a concrete partition id
   * @param partitionId string partition identifier
   * @return PartitionId
   */
  public static PartitionId partition(String partitionId) {
    if (partitionId == null) {
      return null;
    }
    return new PartitionId(PartitionId.extractResourceId(partitionId),
        PartitionId.stripResourceId(partitionId));
  }

  /**
   * Get a concrete partition id
   * @param resourceId resource identifier
   * @param partitionSuffix partition identifier relative to a resource
   * @return PartitionId
   */
  public static PartitionId partition(ResourceId resourceId, String partitionSuffix) {
    return new PartitionId(resourceId, partitionSuffix);
  }

  /**
   * Get a concrete participant id
   * @param participantId string participant identifier
   * @return ParticipantId
   */
  public static ParticipantId participant(String participantId) {
    if (participantId == null) {
      return null;
    }
    return new ParticipantId(participantId);
  }

  /**
   * Get a concrete session id
   * @param sessionId string session identifier
   * @return SessionId
   */
  public static SessionId session(String sessionId) {
    if (sessionId == null) {
      return null;
    }
    return new SessionId(sessionId);
  }

  /**
   * Get a concrete process id
   * @param procId string process identifier (e.g. pid@host)
   * @return ProcId
   */
  public static ProcId process(String processId) {
    if (processId == null) {
      return null;
    }
    return new ProcId(processId);
  }

  /**
   * Get a concrete state model definition id
   * @param stateModelDefId string state model identifier
   * @return StateModelDefId
   */
  public static StateModelDefId stateModelDef(String stateModelDefId) {
    if (stateModelDefId == null) {
      return null;
    }
    return new StateModelDefId(stateModelDefId);
  }

  /**
   * @param stateModelFactoryId
   * @return
   */
  public static StateModelFactoryId stateModelFactory(String stateModelFactoryId) {
    if (stateModelFactoryId == null) {
      return null;
    }
    return new StateModelFactoryId(stateModelFactoryId);
  }

  /**
   * Get a concrete message id
   * @param messageId string message identifier
   * @return MsgId
   */
  public static MessageId message(String messageId) {
    if (messageId == null) {
      return null;
    }
    return new MessageId(messageId);
  }
}
