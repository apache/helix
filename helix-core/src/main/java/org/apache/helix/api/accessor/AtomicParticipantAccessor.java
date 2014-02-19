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

import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.api.Participant;
import org.apache.helix.api.Scope;
import org.apache.helix.api.config.ParticipantConfig;
import org.apache.helix.api.id.ClusterId;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.lock.HelixLock;
import org.apache.helix.lock.HelixLockable;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

/**
 * An atomic version of the ParticipantAccessor. If atomic operations are required, use instances of
 * this class. Atomicity is not guaranteed when using instances of ParticipantAccessor alongside
 * instances of this class. Furthermore, depending on the semantics of the lock, lock acquisition
 * may fail, in which case users should handle the return value of each function if necessary. <br/>
 * <br/>
 * Using this class is quite expensive; it should thus be used sparingly and only in systems where
 * contention on these operations is expected. For most systems running Helix, this is typically not
 * the case.
 */
public class AtomicParticipantAccessor extends ParticipantAccessor {
  private static final Logger LOG = Logger.getLogger(AtomicParticipantAccessor.class);

  private final HelixLockable _lockProvider;

  /**
   * Non-atomic instance to protect against reentrant locking via polymorphism
   */
  private final ParticipantAccessor _participantAccessor;

  /**
   * Instantiate the accessor
   * @param clusterId the cluster to access
   * @param accessor a HelixDataAccessor for the physical properties
   * @param lockProvider a lock provider
   */
  public AtomicParticipantAccessor(ClusterId clusterId, HelixDataAccessor accessor,
      HelixLockable lockProvider) {
    super(clusterId, accessor);
    _lockProvider = lockProvider;
    _participantAccessor = new ParticipantAccessor(clusterId, accessor);
  }

  @Override
  boolean enableParticipant(ParticipantId participantId, boolean isEnabled) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.enableParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public Participant readParticipant(ParticipantId participantId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.readParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  public boolean setParticipant(ParticipantConfig participantConfig) {
    if (participantConfig == null) {
      LOG.error("participant config cannot be null");
      return false;
    }
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantConfig.getId()));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.setParticipant(participantConfig);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public ParticipantConfig updateParticipant(ParticipantId participantId,
      ParticipantConfig.Delta participantDelta) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.updateParticipant(participantId, participantDelta);
      } finally {
        lock.unlock();
      }
    }
    return null;
  }

  @Override
  boolean dropParticipant(ParticipantId participantId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.dropParticipant(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  public void insertMessagesToParticipant(ParticipantId participantId,
      Map<MessageId, Message> msgMap) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        _participantAccessor.insertMessagesToParticipant(participantId, msgMap);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  public void updateMessageStatus(ParticipantId participantId, Map<MessageId, Message> msgMap) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        _participantAccessor.updateMessageStatus(participantId, msgMap);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  public void deleteMessagesFromParticipant(ParticipantId participantId, Set<MessageId> msgIdSet) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        _participantAccessor.deleteMessagesFromParticipant(participantId, msgIdSet);
      } finally {
        lock.unlock();
      }
    }
    return;
  }

  @Override
  public boolean initParticipantStructure(ParticipantId participantId) {
    ClusterId clusterId = clusterId();
    HelixLock lock = _lockProvider.getLock(clusterId, Scope.participant(participantId));
    boolean locked = lock.lock();
    if (locked) {
      try {
        return _participantAccessor.initParticipantStructure(participantId);
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  @Override
  protected ResourceAccessor resourceAccessor() {
    ClusterId clusterId = clusterId();
    HelixDataAccessor accessor = dataAccessor();
    return new AtomicResourceAccessor(clusterId, accessor, _lockProvider);
  }
}
