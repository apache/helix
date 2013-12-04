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

import org.apache.helix.api.id.ContextId;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;

/**
 * An interface for getting and setting {@link ControllerContext} objects, which will eventually
 * be persisted and acessible across runs of the controller pipeline.
 */
public class ControllerContextProvider {
  private static final Logger LOG = Logger.getLogger(ControllerContextProvider.class);

  private Map<ContextId, ControllerContext> _persistedContexts;
  private Map<ContextId, ControllerContext> _pendingContexts;

  /**
   * Instantiate with already-persisted controller contexts
   * @param contexts
   */
  public ControllerContextProvider(Map<ContextId, ControllerContext> contexts) {
    _persistedContexts = contexts != null ? contexts : new HashMap<ContextId, ControllerContext>();
    _pendingContexts = Maps.newHashMap();
  }

  /**
   * Get a base ControllerContext
   * @param contextId the context id to look up
   * @return a ControllerContext, or null if not found
   */
  public ControllerContext getControllerContext(ContextId contextId) {
    return getControllerContext(contextId, ControllerContext.class);
  }

  /**
   * Get a typed ControllerContext
   * @param contextId the context id to look up
   * @param contextClass the class which the context should be returned as
   * @return a typed ControllerContext, or null if no context with given id is available for this
   *         type
   */
  public <T extends ControllerContext> T getControllerContext(ContextId contextId,
      Class<T> contextClass) {
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
  public void putControllerContext(ContextId contextId, ControllerContext context) {
    putControllerContext(contextId, context, true);
  }

  /**
   * Put a controller context, specifying overwrite behavior
   * @param contextId the id to set
   * @param context the context object
   * @param overwriteAllowed true if existing objects can be overwritten, false otherwise
   * @return true if saved, false if an object with that id exists and overwrite is not allowed
   */
  public boolean putControllerContext(ContextId contextId, ControllerContext context,
      boolean overwriteAllowed) {
    if (overwriteAllowed || !exists(contextId)) {
      _pendingContexts.put(contextId, context);
      return true;
    }
    return false;
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
   * Get all contexts that have been put, but not yet persisted
   * @return a map of context id to context
   */
  public Map<ContextId, ControllerContext> getPendingContexts() {
    return _pendingContexts;
  }
}
