package org.apache.helix.controller.provisioner;

import java.util.List;

import org.apache.helix.api.Participant;

/**
 * 
 *
 */
public class TargetProviderInput {

  List<ContainerSpec> containersToBeAcquired;
  List<Participant> existingParticipants;
}
