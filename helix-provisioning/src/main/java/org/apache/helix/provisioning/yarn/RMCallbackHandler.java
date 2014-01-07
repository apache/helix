package org.apache.helix.provisioning.yarn;

import java.util.List;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
  /**
   * 
   */
  private final GenericApplicationMaster _genericApplicationMaster;

  /**
   * @param genericApplicationMaster
   */
  RMCallbackHandler(GenericApplicationMaster genericApplicationMaster) {
    _genericApplicationMaster = genericApplicationMaster;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onContainersCompleted(List<ContainerStatus> completedContainers) {
    GenericApplicationMaster.LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
    for (ContainerStatus containerStatus : completedContainers) {
      GenericApplicationMaster.LOG.info("Got container status for containerID=" + containerStatus.getContainerId()
          + ", state=" + containerStatus.getState() + ", exitStatus="
          + containerStatus.getExitStatus() + ", diagnostics=" + containerStatus.getDiagnostics());

      // non complete containers should not be here
      assert (containerStatus.getState() == ContainerState.COMPLETE);

      // increment counters for completed/failed containers
      int exitStatus = containerStatus.getExitStatus();
      if (0 != exitStatus) {
        // container failed
        if (ContainerExitStatus.ABORTED != exitStatus) {
          // shell script failed
          // counts as completed
          _genericApplicationMaster.numCompletedContainers.incrementAndGet();
          _genericApplicationMaster.numFailedContainers.incrementAndGet();
        } else {
          // container was killed by framework, possibly preempted
          // we should re-try as the container was lost for some reason
          _genericApplicationMaster.numAllocatedContainers.decrementAndGet();
          _genericApplicationMaster.numRequestedContainers.decrementAndGet();
          // we do not need to release the container as it would be done
          // by the RM
        }
      } else {
        // nothing to do
        // container completed successfully
        _genericApplicationMaster.numCompletedContainers.incrementAndGet();
        GenericApplicationMaster.LOG.info("Container completed successfully." + ", containerId="
            + containerStatus.getContainerId());
      }
    }
  }

  @Override
  public void onContainersAllocated(List<Container> allocatedContainers) {
    GenericApplicationMaster.LOG.info("Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
    _genericApplicationMaster.numAllocatedContainers.addAndGet(allocatedContainers.size());
    for (Container allocatedContainer : allocatedContainers) {
      GenericApplicationMaster.LOG.info("Launching shell command on a new container." + ", containerId="
          + allocatedContainer.getId() + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());
      // + ", containerToken"
      // +allocatedContainer.getContainerToken().getIdentifier().toString());

      LaunchContainerRunnable runnableLaunchContainer =
          new LaunchContainerRunnable(_genericApplicationMaster, allocatedContainer, _genericApplicationMaster.containerListener);
      Thread launchThread = new Thread(runnableLaunchContainer);

      // launch and start the container on a separate thread to keep
      // the main thread unblocked
      // as all containers may not be allocated at one go.
      _genericApplicationMaster.launchThreads.add(launchThread);
      launchThread.start();
    }
  }

  @Override
  public void onShutdownRequest() {
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
  }

  @Override
  public float getProgress() {
    // set progress to deliver to RM on next heartbeat
    return 0.5f;
  }

  @Override
  public void onError(Throwable e) {
    _genericApplicationMaster.amRMClient.stop();
  }
}