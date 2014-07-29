package org.apache.helix.api.accessor;

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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Resource;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ClusterConfig;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.HelixLockable;
import org.apache.log4j.Logger;

/**
 * An atomic version of the ClusterAccessor. If atomic operations are required, use instances of
 * this class. Atomicity is not guaranteed when using instances of ClusterAccessor alongside
 * instances of this class. Furthermore, depending on the semantics of the lock, lock acquisition
 * may fail, in which case users should handle the return value of each function if necessary. <br/>
 * <br/>
 * Using this class is quite expensive; it should thus be used sparingly and only in systems where
 * contention on these operations is expected. For most systems running Helix, this is typically not
 * the case.
 */
public class AtomicClusterAccessor extends ClusterAccessor {
  private static final Logger LOG = Logger.getLogger(AtomicClusterAccessor.class);

  private final HelixLockable _lockProvider;

  /**
   * Non-atomic instance to protect against reentrant locking via polymorphism
   */
  private final ClusterAccessor _clusterAccessor;

  /**
   * Instantiate the accessor
   * @param clusterId the cluster to access
   * @param accessor a HelixDataAccessor for the physical properties
   * @param lockProvider a lock provider
   */
  public AtomicClusterAccessor(ClusterId clusterId, HelixDataAccessor accessor,
      HelixLockable lockProvider) {
    super(clusterId, accessor);
    _lockProvider = lockProvider;
    _clusterAccessor = new ClusterAccessor(clusterId, accessor);
  }

  @Override
  public boolean createCluster(ClusterConfig cluster) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.cluster(clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.createCluster(cluster);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean dropCluster() {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.cluster(clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.dropCluster();
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public Cluster readCluster() {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.cluster(clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.readCluster();
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public boolean addParticipant(ParticipantConfig participant) {
    if (participant == null) {
      LOG.error("Participant config cannot be null");
      return false;
    }
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participant.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.addParticipant(participant);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean dropParticipant(ParticipantId participantId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.dropParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean addResource(ResourceConfig resource) {
    if (resource == null) {
      LOG.error("Resource config cannot be null");
      return false;
    }
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.resource(resource.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.addResource(resource);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean dropResource(ResourceId resourceId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.dropResource(resourceId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public ClusterConfig updateCluster(ClusterConfig.Delta clusterDelta) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.cluster(clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.updateCluster(clusterDelta);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public Participant readParticipant(ParticipantId participantId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.readParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public ParticipantConfig updateParticipant(ParticipantId participantId,
      ParticipantConfig.Delta participantDelta) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.updateParticipant(participantId, participantDelta);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public Resource readResource(ResourceId resourceId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.readResource(resourceId);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public ResourceConfig updateResource(ResourceId resourceId, ResourceConfig.Delta resourceDelta) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.updateResource(resourceId, resourceDelta);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }
}
