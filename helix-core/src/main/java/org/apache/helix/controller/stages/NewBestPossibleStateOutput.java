package org.apache.helix.controller.stages;

import java.util.HashMap;
import java.util.Map;

import org.apache.helix.api.ResourceId;
import org.apache.helix.model.ResourceAssignment;

public class NewBestPossibleStateOutput {

  Map<ResourceId, ResourceAssignment> _resourceAssignmentMap;

  public NewBestPossibleStateOutput() {
    _resourceAssignmentMap = new HashMap<ResourceId, ResourceAssignment>();
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
}
