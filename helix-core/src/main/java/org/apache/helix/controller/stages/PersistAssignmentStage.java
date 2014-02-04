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

import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Resource;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.model.ResourceAssignment;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

/**
 * Persist the ResourceAssignment of each resource that went through rebalancing
 */
public class PersistAssignmentStage extends AbstractBaseStage {
  private static final Logger LOG = Logger.getLogger(PersistAssignmentStage.class);

  @Override
  public void process(ClusterEvent event) throws Exception {
    LOG.info("START PersistAssignmentStage.process()");
    long startTime = System.currentTimeMillis();

    ClusterDataCache cache = event.getAttribute("ClusterDataCache");
    if (cache.assignmentWriteEnabled()) {
      Cluster cluster = event.getAttribute("Cluster");
      HelixManager helixManager = event.getAttribute("helixmanager");
      HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
      PropertyKey.Builder keyBuilder = accessor.keyBuilder();
      BestPossibleStateOutput assignments =
          event.getAttribute(AttributeName.BEST_POSSIBLE_STATE.toString());
      List<ResourceAssignment> changedAssignments = Lists.newLinkedList();
      List<PropertyKey> changedKeys = Lists.newLinkedList();
      for (ResourceId resourceId : assignments.getAssignedResources()) {
        ResourceAssignment assignment = assignments.getResourceAssignment(resourceId);
        Resource resource = cluster.getResource(resourceId);
        boolean toAdd = false;
        if (resource != null) {
          ResourceAssignment existAssignment = resource.getResourceAssignment();
          if (existAssignment == null || !existAssignment.equals(assignment)) {
            toAdd = true;
          }
        } else {
          toAdd = true;
        }
        if (toAdd) {
          changedAssignments.add(assignment);
          changedKeys.add(keyBuilder.resourceAssignment(resourceId.toString()));
        }
      }

      // update as a batch operation
      if (changedAssignments.size() > 0) {
        accessor.setChildren(changedKeys, changedAssignments);
      }
    }

    long endTime = System.currentTimeMillis();
    LOG.info("END PersistAssignmentStage.process(), took " + (endTime - startTime) + " ms");
  }
}
