package org.apache.helix.controller.stages;

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.api.accessor.ResourceAccessor;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.ResourceAssignment;

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
 * Persist the ResourceAssignment of each resource that went through rebalancing
 */
public class PersistAssignmentStage extends AbstractBaseStage {
  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager helixManager = event.getAttribute("helixmanager");
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
    ResourceAccessor resourceAccessor = new ResourceAccessor(accessor);
    NewBestPossibleStateOutput assignments =
        event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
    for (ResourceId resourceId : assignments.getAssignedResources()) {
      ResourceAssignment assignment = assignments.getResourceAssignment(resourceId);
      resourceAccessor.setResourceAssignment(resourceId, assignment);
    }
  }
}
