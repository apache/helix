package org.apache.helix.controller.provisioner;

import java.util.List;

import org.apache.helix.api.Participant;

public class TargetProviderResponse {

  List<ContainerSpec> containersToAcquire;

  List<Participant> containersToRelease;

  List<Participant> containersToStop;

  List<Participant> containersToStart;

  public List<ContainerSpec> getContainersToAcquire() {
    return containersToAcquire;
  }

  public void setContainersToAcquire(List<ContainerSpec> containersToAcquire) {
    this.containersToAcquire = containersToAcquire;
  }

  public List<Participant> getContainersToRelease() {
    return containersToRelease;
  }

  public void setContainersToRelease(List<Participant> containersToRelease) {
    this.containersToRelease = containersToRelease;
  }

  public List<Participant> getContainersToStop() {
    return containersToStop;
  }

  public void setContainersToStop(List<Participant> containersToStop) {
    this.containersToStop = containersToStop;
  }

  public List<Participant> getContainersToStart() {
    return containersToStart;
  }

  public void setContainersToStart(List<Participant> containersToStart) {
    this.containersToStart = containersToStart;
  }

}
