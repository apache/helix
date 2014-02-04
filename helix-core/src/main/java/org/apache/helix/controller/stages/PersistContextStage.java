package org.apache.helix.controller.stages;

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
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.id.ContextId;
import org.apache.helix.controller.context.ControllerContext;
import org.apache.helix.controller.context.ControllerContextHolder;
import org.apache.helix.controller.context.ControllerContextProvider;
import org.apache.helix.controller.pipeline.AbstractBaseStage;

import com.google.common.collect.Lists;

/**
 * Persist all dirty contexts set in the controller pipeline
 */
public class PersistContextStage extends AbstractBaseStage {
  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager helixManager = event.getAttribute("helixmanager");
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    ControllerContextProvider contextProvider =
        event.getAttribute(AttributeName.CONTEXT_PROVIDER.toString());

    // remove marked contexts
    Set<ContextId> removedContexts = contextProvider.getRemovedContexts();
    List<String> removedPaths = Lists.newLinkedList();
    for (ContextId contextId : removedContexts) {
      removedPaths.add(keyBuilder.controllerContext(contextId.stringify()).getPath());
    }
    if (removedPaths.size() > 0) {
      accessor.getBaseDataAccessor().remove(removedPaths, 0);
    }

    // persist pending contexts
    Map<ContextId, ControllerContext> pendingContexts = contextProvider.getPendingContexts();
    List<PropertyKey> keys = Lists.newArrayList();
    List<ControllerContextHolder> properties = Lists.newArrayList();
    for (ContextId contextId : pendingContexts.keySet()) {
      ControllerContextHolder holder = new ControllerContextHolder(pendingContexts.get(contextId));
      if (holder != null) {
        keys.add(keyBuilder.controllerContext(contextId.stringify()));
        properties.add(holder);
      }
    }

    if (keys.size() > 0) {
      accessor.setChildren(keys, properties);
    }
  }
}
