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

import java.util.Map;
import java.util.Set;

import org.apache.helix.api.id.ResourceId;
import org.apache.helix.model.ResourceAssignment;

import com.google.common.collect.Maps;

public class BestPossibleStateOutput {

  Map<ResourceId, ResourceAssignment> _resourceAssignmentMap;

  public BestPossibleStateOutput() {
    _resourceAssignmentMap = Maps.newHashMap();
  }

  /**
   * Set the computed resource assignment for a resource
   * @param resourceId the resource to set
   * @param resourceAssignment the computed assignment
   */
  public void setResourceAssignment(ResourceId resourceId, ResourceAssignment resourceAssignment) {
    _resourceAssignmentMap.put(resourceId, resourceAssignment);
  }

  /**
   * Get the resource assignment computed for a resource
   * @param resourceId resource to look up
   * @return ResourceAssignment computed by the best possible state calculation
   */
  public ResourceAssignment getResourceAssignment(ResourceId resourceId) {
    return _resourceAssignmentMap.get(resourceId);
  }

  /**
   * Get all of the resources currently assigned
   * @return set of assigned resource ids
   */
  public Set<ResourceId> getAssignedResources() {
    return _resourceAssignmentMap.keySet();
  }

  @Override
  public String toString() {
    return _resourceAssignmentMap.toString();
  }
}
