package org.apache.helix.provisioning.yarn;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.util.Records;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.TargetProviderResponse;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class YarnProvisioner implements Provisioner {

  private static final Log LOG = LogFactory.getLog(YarnProvisioner.class);
  static GenericApplicationMaster applicationMaster;
  static ListeningExecutorService service = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
  Map<ContainerId, Container> allocatedContainersMap = new HashMap<ContainerId, Container>();

  @Override
  public ListenableFuture<ContainerId> allocateContainer(ContainerSpec spec) {
    ContainerRequest containerAsk = setupContainerAskForRM(spec);
    ListenableFuture<ContainerAskResponse> requestNewContainer =
        applicationMaster.acquireContainer(containerAsk);
    return Futures.transform(requestNewContainer, new Function<ContainerAskResponse, ContainerId>() {
      @Override
      public ContainerId apply(ContainerAskResponse containerAskResponse) {
        ContainerId helixContainerId =
            ContainerId.from(containerAskResponse.getContainer().getId().toString());
        allocatedContainersMap.put(helixContainerId, containerAskResponse.getContainer());
        return helixContainerId;
      }
    });
  }

  @Override
  public ListenableFuture<Boolean> deallocateContainer(ContainerId containerId) {
    ListenableFuture<ContainerReleaseResponse> releaseContainer =
        applicationMaster.releaseContainer(allocatedContainersMap.get(containerId));
    return Futures.transform(releaseContainer, new Function<ContainerReleaseResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerReleaseResponse response) {
        return response != null;
      }
    }, service);
  }

  @Override
  public ListenableFuture<Boolean> startContainer(final ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    ListenableFuture<ContainerLaunchResponse> future = applicationMaster.launchContainer(container, containerLaunchContext);
    return Futures.transform(future, new Function<ContainerLaunchResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerLaunchResponse response) {
        return response != null;
      }
    }, service);
  }

  @Override
  public ListenableFuture<Boolean> stopContainer(final ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    ListenableFuture<ContainerStopResponse> future = applicationMaster.stopContainer(container);
    return Futures.transform(future, new Function<ContainerStopResponse, Boolean>() {
      @Override
      public Boolean apply(ContainerStopResponse response) {
        return response != null;
      }
    }, service);
  }

  @Override
  public ContainerState getContainerState(ContainerId containerId) {
    return null;
  }

  @Override
  public void init(HelixManager helixManager) {

  }

  @Override
  public TargetProviderResponse evaluateExistingContainers(Cluster cluster, ResourceId resourceId,
      Collection<Participant> participants) {
    // TODO Auto-generated method stub
    return null;
  }

  private ContainerRequest setupContainerAskForRM(ContainerSpec spec) {
    // setup requirements for hosts
    // using * as any host will do for the distributed shell app
    // set the priority for the request
    Priority pri = Records.newRecord(Priority.class);
    int requestPriority = 0;
    // TODO - what is the range for priority? how to decide?
    pri.setPriority(requestPriority);

    // Set up resource type requirements
    // For now, only memory is supported so we set memory requirements
    Resource capability = Records.newRecord(Resource.class);
    int memory = 1024;
    capability.setMemory(memory);

    ContainerRequest request = new ContainerRequest(capability, null, null, pri);
    LOG.info("Requested container ask: " + request.toString());
    return request;
  }

}
