package org.apache.helix.controller.context;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.helix.api.id.ContextId;
import org.apache.log4j.Logger;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An interface for getting and setting {@link ControllerContext} objects, which will eventually
 * be persisted and accessible across runs of the controller pipeline.
 */
public class ControllerContextProvider {
  private static final Logger LOG = Logger.getLogger(ControllerContextProvider.class);

  private Map<ContextId, ControllerContext> _persistedContexts;
  private Map<ContextId, ControllerContext> _pendingContexts;
  private Set<ContextId> _removedContexts;

  /**
   * Instantiate with already-persisted controller contexts
   * @param contexts
   */
  public ControllerContextProvider(Map<ContextId, ControllerContext> contexts) {
    _persistedContexts = contexts != null ? contexts : new HashMap<ContextId, ControllerContext>();
    _pendingContexts = Maps.newHashMap();
    _removedContexts = Sets.newHashSet();
  }

  /**
   * Get a base ControllerContext
   * @param contextId the context id to look up
   * @return a ControllerContext, or null if not found
   */
  public ControllerContext getContext(ContextId contextId) {
    return getContext(contextId, ControllerContext.class);
  }

  /**
   * Get a typed ControllerContext
   * @param contextId the context id to look up
   * @param contextClass the class which the context should be returned as
   * @return a typed ControllerContext, or null if no context with given id is available for this
   *         type
   */
  public <T extends ControllerContext> T getContext(ContextId contextId, Class<T> contextClass) {
    try {
      if (_pendingContexts.containsKey(contextId)) {
        return contextClass.cast(_pendingContexts.get(contextId));
      } else if (_persistedContexts.containsKey(contextId)) {
        return contextClass.cast(_persistedContexts.get(contextId));
      }
    } catch (ClassCastException e) {
      LOG.error("Could not convert context " + contextId + " into " + contextClass.getName());
    }
    return null;
  }

  /**
   * Put a controller context, overwriting any existing ones
   * @param contextId the id to set
   * @param context the context object
   */
  public void putContext(ContextId contextId, ControllerContext context) {
    putContext(contextId, context, true);
  }

  /**
   * Put a controller context, specifying overwrite behavior
   * @param contextId the id to set
   * @param context the context object
   * @param overwriteAllowed true if existing objects can be overwritten, false otherwise
   * @return true if saved, false if an object with that id exists and overwrite is not allowed
   */
  public boolean putContext(ContextId contextId, ControllerContext context, boolean overwriteAllowed) {
    // avoid persisting null contexts
    if (context == null) {
      LOG.error("Cannot save a null context, id: " + contextId);
      return false;
    }
    if (overwriteAllowed || !exists(contextId)) {
      _pendingContexts.put(contextId, context);
      if (_removedContexts.contains(contextId)) {
        // no need to mark as removed if it's being added again
        _removedContexts.remove(contextId);
      }
      return true;
    }
    return false;
  }

  /**
   * Remove a controller context
   * @param contextId the id to remove
   * @return ControllerContext that was removed, or null
   */
  public ControllerContext removeContext(ContextId contextId) {
    ControllerContext removed = null;
    if (_persistedContexts.containsKey(contextId)) {
      removed = _persistedContexts.remove(contextId);
    }
    if (_pendingContexts.containsKey(contextId)) {
      // check pending second since it might overwrite a persisted context
      removed = _pendingContexts.remove(contextId);
    }
    // mark as removed even if pending; this is so that remove, put, remove works
    _removedContexts.add(contextId);
    return removed;
  }

  /**
   * Check if a context exists
   * @param contextId the id to look up
   * @return true if a context exists with that id, false otherwise
   */
  public boolean exists(ContextId contextId) {
    return _persistedContexts.containsKey(contextId) || _pendingContexts.containsKey(contextId);
  }

  /**
   * Get all contexts, both persisted and pending
   * @return an immutable map of context id to context
   */
  public Map<ContextId, ControllerContext> getContexts() {
    Map<ContextId, ControllerContext> aggregateMap = Maps.newHashMap();
    aggregateMap.putAll(_persistedContexts);
    aggregateMap.putAll(_pendingContexts);
    return ImmutableMap.copyOf(aggregateMap);
  }

  /**
   * Get all contexts that have been put, but not yet persisted
   * @return an immutable map of context id to context
   */
  public Map<ContextId, ControllerContext> getPendingContexts() {
    return ImmutableMap.copyOf(_pendingContexts);
  }

  /**
   * Get all context ids that have been marked for removal
   * @return a set of context ids
   */
  public Set<ContextId> getRemovedContexts() {
    return ImmutableSet.copyOf(_removedContexts);
  }
}
