package org.apache.helix.controller.stages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.util.HelixUtil;

/**
 * This stage will manager the container allocation/deallocation needed for a
 * specific resource.<br/>
 * It does the following <br/>
 * From the idealstate, it gets ContainerTargetProvider and ContainerProvider <br/>
 * ContainerTargetProviderFactory will provide the number of containers needed
 * for a resource <br/>
 * ContainerProvider will provide the ability to allocate, deallocate, start,
 * stop container <br/>
 */
public class ContainerProvisioningStage extends AbstractBaseStage {

  Map<ResourceId, Provisioner> _provisionerMap = new HashMap<ResourceId, Provisioner>();

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager helixManager = event.getAttribute("helixmanager");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    for (ResourceId resourceId : resourceMap.keySet()) {
      ResourceConfig resourceConfig = resourceMap.get(resourceId);
      ProvisionerConfig provisionerConfig = resourceConfig.getProvisionerConfig();
      if (provisionerConfig != null) {
        Provisioner provisioner;
        provisioner = _provisionerMap.get(resourceId);

        if (provisioner == null) {
          String provisionerClass = resourceConfig.getProvisionerClass();
          provisioner =
              (Provisioner) (HelixUtil.loadClass(getClass(), provisionerClass).newInstance());
          _provisionerMap.put(resourceId, provisioner);
        }

        Cluster cluster = event.getAttribute("clusterDataCache");
        Collection<Participant> participants = cluster.getParticipantMap().values();
        
        //Participants registered in helix
        //Give those participants to targetprovider
        //Provide the response that contains, new containerspecs, containers to be released, containers to be stopped
        //call the respective provisioner to allocate and start the container. 
        //Each container is then started its state is changed from any place.
        //The target provider is given the state of container and asked for its new state. For each state there is a corresponding handler function.
        
        //TargetProvider should be stateless, given the state of cluster and existing participants it should return the same result
        TargetProviderResponse response = provisioner.evaluateExistingContainers(cluster,resourceId, participants);
        
        //start new containers
        for(Participant participant: response.getContainersToStart()){
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          ContainerId containerId = ContainerId.from(containerIdStr);
          //create the helix participant and add it to cluster
          provisioner.startContainer(containerId);
        }
        
        //allocate new containers
        for(ContainerSpec spec: response.getContainersToAcquire()){
          //create a new Participant, attach the container spec
          //create a helix_participant in ACQUIRING STATE
          ContainerId containerId = provisioner.allocateContainer(spec);
          //create the helix participant and add it to cluster
          provisioner.startContainer(containerId);
        }
        
        //release containers
        for(Participant participant: response.getContainersToRelease()){
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          ContainerId containerId = ContainerId.from(containerIdStr);
          //disable the node first
          provisioner.stopContainer(containerId);
          //this will change the container state
          provisioner.deallocateContainer(containerId);
          //remove the participant
        }

        //stop but dont remove
        for(Participant participant: participants){
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          ContainerId containerId = ContainerId.from(containerIdStr);
          provisioner.stopContainer(containerId);
        }
      }
    }
  }
}
