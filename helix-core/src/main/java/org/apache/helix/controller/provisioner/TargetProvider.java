package org.apache.helix.controller.provisioner;

import java.util.Collection;
import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.id.ResourceId;

public interface TargetProvider {

  public void init(HelixManager helixManager);

 /**
  * @param cluster
  * @param resourceId ResourceId name of the resource
  * @param participants
  * @return
  */
  TargetProviderResponse evaluateExistingContainers(Cluster cluster,ResourceId resourceId,
      Collection<Participant> participants);

}
