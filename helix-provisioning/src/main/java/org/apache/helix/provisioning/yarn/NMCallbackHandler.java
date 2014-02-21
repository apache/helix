package org.apache.helix.provisioning.yarn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.SettableFuture;

@VisibleForTesting
class NMCallbackHandler implements NMClientAsync.CallbackHandler {

  private Logger LOG = Logger.getLogger(NMCallbackHandler.class);
  private ConcurrentMap<ContainerId, Container> containers =
      new ConcurrentHashMap<ContainerId, Container>();
  private final GenericApplicationMaster applicationMaster;

  public NMCallbackHandler(GenericApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
  }

  public void addContainer(ContainerId containerId, Container container) {
    containers.putIfAbsent(containerId, container);
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    LOG.info("Succeeded to stop Container " + containerId);
    Container container = containers.get(containerId);
    if (container != null) {
      applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
    }
    SettableFuture<ContainerStopResponse> settableFuture =
        applicationMaster.containerStopMap.get(containerId);
    ContainerStopResponse value = new ContainerStopResponse();
    settableFuture.set(value);
    containers.remove(containerId);
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    LOG.info("Container Status: id=" + containerId + ", status=" + containerStatus);
  }

  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    LOG.debug("Succeeded to start Container " + containerId);

    Container container = containers.get(containerId);
    if (container != null) {
      applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
    }
    SettableFuture<ContainerLaunchResponse> settableFuture =
        applicationMaster.containerLaunchResponseMap.get(containerId);
    ContainerLaunchResponse value = new ContainerLaunchResponse();
    settableFuture.set(value);
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to start Container " + containerId);
    containers.remove(containerId);
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to query the status of Container " + containerId);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to stop Container " + containerId);
    containers.remove(containerId);
  }
}
