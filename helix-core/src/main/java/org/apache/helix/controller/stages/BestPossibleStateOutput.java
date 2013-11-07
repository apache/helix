package org.apache.helix.controller.stages;

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
