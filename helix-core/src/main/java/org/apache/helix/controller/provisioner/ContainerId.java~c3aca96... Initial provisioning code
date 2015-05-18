package org.apache.helix.controller.provisioner;

import org.apache.helix.api.id.Id;

public class ContainerId extends Id {

  String id;

  private ContainerId(String containerId) {
    this.id = containerId;
  }

  @Override
  public String stringify() {
    return id;
  }

  /**
   * Get a concrete partition id
   * @param partitionId string partition identifier
   * @return PartitionId
   */
  public static ContainerId from(String containerId) {
    if (containerId == null) {
      return null;
    }
    return new ContainerId(containerId);
  }

}
