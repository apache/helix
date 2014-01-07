package org.apache.helix.provisioning.yarn;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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

public class YarnProvisioner implements Provisioner {

  private static final Log LOG = LogFactory.getLog(YarnProvisioner.class);
  static GenericApplicationMaster applicationMaster;
  Map<ContainerId, Container> allocatedContainersMap = new HashMap<ContainerId, Container>();

  @Override
  public ContainerId allocateContainer(ContainerSpec spec) {
    ContainerRequest containerAsk = setupContainerAskForRM(spec);
    Future<ContainerAskResponse> requestNewContainer =
        applicationMaster.acquireContainer(containerAsk);
    ContainerAskResponse containerAskResponse;
    try {
      containerAskResponse = requestNewContainer.get();
      ContainerId helixContainerId =
          ContainerId.from(containerAskResponse.getContainer().getId().toString());
      allocatedContainersMap.put(helixContainerId, containerAskResponse.getContainer());
      return helixContainerId;
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public boolean deallocateContainer(ContainerId containerId) {
    Future<ContainerReleaseResponse> releaseContainer =
        applicationMaster.releaseContainer(allocatedContainersMap.get(containerId));
    try {
      releaseContainer.get();
      return true;
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ExecutionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return false;
  }

  @Override
  public boolean startContainer(ContainerId containerId) {
    Container container = allocatedContainersMap.get(containerId);
    ContainerLaunchContext containerLaunchContext = Records.newRecord(ContainerLaunchContext.class);
    applicationMaster.launchContainer(container, containerLaunchContext);
    return false;
  }

  @Override
  public boolean stopContainer(ContainerId containerId) {
    return false;
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
