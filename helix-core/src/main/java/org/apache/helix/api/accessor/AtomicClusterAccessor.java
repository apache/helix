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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An atomic version of the ClusterAccessor. If atomic operations are required, use instances of
 * this class. Atomicity is not guaranteed when using instances of ClusterAccessor alongside
 * instances of this class. Furthermore, depending on the semantics of the lock, lock acquisition
 * may fail, in which case users should handle the return value of each function if necessary.
 */
public class AtomicClusterAccessor extends ClusterAccessor {
  private static final Logger LOG = Logger.getLogger(AtomicClusterAccessor.class);

  private final HelixLockable _lockProvider;
  private final HelixDataAccessor _accessor;
  private final PropertyKey.Builder _keyBuilder;
  private final ClusterId _clusterId;

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
    _accessor = accessor;
    _keyBuilder = accessor.keyBuilder();
    _clusterId = clusterId;
    _clusterAccessor = new ClusterAccessor(clusterId, accessor);
  }

  @Override
  public boolean createCluster(ClusterConfig cluster) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
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
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
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
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
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
  public boolean addParticipantToCluster(ParticipantConfig participant) {
    if (participant == null) {
      LOG.error("Participant config cannot be null");
      return false;
    }
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participant.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.addParticipantToCluster(participant);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean dropParticipantFromCluster(ParticipantId participantId) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.dropParticipantFromCluster(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean addResourceToCluster(ResourceConfig resource) {
    if (resource == null) {
      LOG.error("Resource config cannot be null");
      return false;
    }
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resource.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.addResourceToCluster(resource);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public boolean dropResourceFromCluster(ResourceId resourceId) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.resource(resourceId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _clusterAccessor.dropResourceFromCluster(resourceId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public ClusterConfig updateCluster(ClusterConfig.Delta clusterDelta) {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
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

  /**
   * Read resources atomically. This is resource-atomic, not cluster-atomic
   */
  @Override
  public Map<ResourceId, Resource> readResources() {
    // read resources individually instead of together to maintain the equality link between ideal
    // state and resource config
    Map<ResourceId, Resource> resources = Maps.newHashMap();
    Set<String> idealStateNames =
        Sets.newHashSet(_accessor.getChildNames(_keyBuilder.idealStates()));
    Set<String> resourceConfigNames =
        Sets.newHashSet(_accessor.getChildNames(_keyBuilder.resourceConfigs()));
    resourceConfigNames.addAll(idealStateNames);
    ResourceAccessor accessor = new AtomicResourceAccessor(_clusterId, _accessor, _lockProvider);
    for (String resourceName : resourceConfigNames) {
      ResourceId resourceId = ResourceId.from(resourceName);
      Resource resource = accessor.readResource(resourceId);
      if (resource != null) {
        resources.put(resourceId, resource);
      }
    }
    return resources;
  }

  /**
   * Read participants atomically. This is participant-atomic, not cluster-atomic
   */
  @Override
  public Map<ParticipantId, Participant> readParticipants() {
    // read participants individually to keep configs consistent with current state and messages
    Map<ParticipantId, Participant> participants = Maps.newHashMap();
    ParticipantAccessor accessor =
        new AtomicParticipantAccessor(_clusterId, _accessor, _lockProvider);
    List<String> participantNames = _accessor.getChildNames(_keyBuilder.instanceConfigs());
    for (String participantName : participantNames) {
      ParticipantId participantId = ParticipantId.from(participantName);
      Participant participant = accessor.readParticipant(participantId);
      if (participant != null) {
        participants.put(participantId, participant);
      }
    }
    return participants;
  }

  @Override
  public void initClusterStructure() {
    HelixLock lock = _lockProvider.getLock(_clusterId, Scope.cluster(_clusterId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        _clusterAccessor.initClusterStructure();
      } finally {
        lock.unlock();
      }
    }
  }
}
