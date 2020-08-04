package org.apache.helix.common.caches;

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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.PropertyKey;
import org.apache.helix.common.controllers.ControlContextProvider;
import org.apache.helix.model.CurrentState;
import org.apache.helix.model.LiveInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Cache to hold all CurrentStates of a cluster.
 */
public class CurrentStateCache extends ParticipantStateCache<CurrentState> {
  private static final Logger LOG = LoggerFactory.getLogger(CurrentStateCache.class.getName());
  // If the snapshot is already refreshed with current state data.
  private boolean _initialized = false;
  private CurrentStateSnapshot _snapshot;

  public CurrentStateCache(String clusterName) {
    this(createDefaultControlContextProvider(clusterName));
  }

  public CurrentStateCache(ControlContextProvider contextProvider) {
    super(contextProvider);
    _snapshot = new CurrentStateSnapshot(_participantStateCache);
  }

  @Override
  protected Set<PropertyKey> PopulateParticipantKeys(HelixDataAccessor accessor,
      Map<String, LiveInstance> liveInstanceMap) {
    Set<PropertyKey> participantStateKeys = new HashSet<>();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    for (String instanceName : liveInstanceMap.keySet()) {
      LiveInstance liveInstance = liveInstanceMap.get(instanceName);
      String sessionId = liveInstance.getEphemeralOwner();
      List<String> currentStateNames =
          accessor.getChildNames(keyBuilder.currentStates(instanceName, sessionId));
      for (String currentStateName : currentStateNames) {
        participantStateKeys
            .add(keyBuilder.currentState(instanceName, sessionId, currentStateName));
      }
    }
    return participantStateKeys;
  }

  protected void refreshSnapshot(Map<PropertyKey, CurrentState> newStateCache,
      Map<PropertyKey, CurrentState> participantStateCache, Set<PropertyKey> reloadedKeys) {
    if (_initialized) {
      _snapshot = new CurrentStateSnapshot(newStateCache, participantStateCache, reloadedKeys);
    } else {
      _snapshot = new CurrentStateSnapshot(newStateCache);
      _initialized = true;
    }
  }

  @Override
  public CurrentStateSnapshot getSnapshot() {
    return _snapshot;
  }
}
