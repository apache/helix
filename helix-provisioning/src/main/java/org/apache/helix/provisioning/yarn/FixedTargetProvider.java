package org.apache.helix.provisioning.yarn;

import java.util.Collection;

import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;

public class FixedTargetProvider implements TargetProvider {

  @Override
  public TargetProviderResponse evaluateExistingContainers(Cluster cluster, ResourceId resourceId,
      Collection<Participant> participants) {
    // TODO Auto-generated method stub
    return null;
  }

}
