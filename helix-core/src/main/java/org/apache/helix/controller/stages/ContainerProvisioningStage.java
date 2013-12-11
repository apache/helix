package org.apache.helix.controller.stages;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.PropertyKey;
import org.apache.helix.api.Cluster;
import org.apache.helix.api.Participant;
import org.apache.helix.api.config.ResourceConfig;
import org.apache.helix.api.id.ParticipantId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.controller.pipeline.AbstractBaseStage;
import org.apache.helix.controller.provisioner.ContainerId;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.ProvisionerRef;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

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
  private static final Logger LOG = Logger.getLogger(ContainerProvisioningStage.class);

  Map<ResourceId, Provisioner> _provisionerMap = new HashMap<ResourceId, Provisioner>();

  @Override
  public void process(ClusterEvent event) throws Exception {
    HelixManager helixManager = event.getAttribute("helixmanager");
    Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
    PropertyKey.Builder keyBuilder = accessor.keyBuilder();
    for (ResourceId resourceId : resourceMap.keySet()) {
      ResourceConfig resourceConfig = resourceMap.get(resourceId);
      ProvisionerConfig provisionerConfig = resourceConfig.getProvisionerConfig();
      if (provisionerConfig != null) {
        Provisioner provisioner;
        provisioner = _provisionerMap.get(resourceId);

        // instantiate and cache a provisioner if there isn't one already cached
        if (provisioner == null) {
          ProvisionerRef provisionerRef = provisionerConfig.getProvisionerRef();
          if (provisionerRef != null) {
            provisioner = provisionerRef.getProvisioner();
          }
          if (provisioner != null) {
            provisioner.init(helixManager);
            _provisionerMap.put(resourceId, provisioner);
          } else {
            LOG.error("Resource " + resourceId + " does not have a valid provisioner class!");
            break;
          }
        }

        Cluster cluster = event.getAttribute("clusterDataCache");
        Collection<Participant> participants = cluster.getParticipantMap().values();

        // Participants registered in helix
        // Give those participants to targetprovider
        // Provide the response that contains, new containerspecs, containers to be released,
        // containers to be stopped
        // call the respective provisioner to allocate and start the container.
        // Each container is then started its state is changed from any place.
        // The target provider is given the state of container and asked for its new state. For each
        // state there is a corresponding handler function.

        // TargetProvider should be stateless, given the state of cluster and existing participants
        // it should return the same result
        TargetProviderResponse response =
            provisioner.evaluateExistingContainers(cluster, resourceId, participants);

        // random participant id
        ParticipantId participantId = ParticipantId.from(UUID.randomUUID().toString());

        // allocate new containers
        for (ContainerSpec spec : response.getContainersToAcquire()) {
          // create a new Participant, attach the container spec
          InstanceConfig instanceConfig = new InstanceConfig(participantId);
          instanceConfig.setContainerSpec(spec);
          // create a helix_participant in ACQUIRING state
          instanceConfig.setContainerState(ContainerState.ACQUIRING);
          // create the helix participant and add it to cluster
          helixAdmin.addInstance(cluster.getId().toString(), instanceConfig);

          ContainerId containerId = provisioner.allocateContainer(spec);
          InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          existingInstance.setContainerId(containerId);
          accessor.setProperty(keyBuilder.instanceConfig(participantId.toString()),
              existingInstance);
        }

        // start new containers
        for (Participant participant : response.getContainersToStart()) {
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          ContainerId containerId = ContainerId.from(containerIdStr);
          InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          existingInstance.setContainerState(ContainerState.CONNECTING);
          accessor.setProperty(keyBuilder.instanceConfig(participantId.toString()),
              existingInstance);
          // create the helix participant and add it to cluster
          provisioner.startContainer(containerId);
          existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          existingInstance.setContainerState(ContainerState.ACTIVE);
          accessor.setProperty(keyBuilder.instanceConfig(participantId.toString()),
              existingInstance);
        }

        // release containers
        for (Participant participant : response.getContainersToRelease()) {
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          ContainerId containerId = ContainerId.from(containerIdStr);
          // this will change the container state
          provisioner.deallocateContainer(containerId);
          // remove the participant
          InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          helixAdmin.dropInstance(cluster.getId().toString(), existingInstance);
        }

        // stop but don't remove
        for (Participant participant : participants) {
          String containerIdStr = participant.getUserConfig().getSimpleField("ContainerId");
          // disable the node first
          // TODO: get the participant id from the container id
          InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          existingInstance.setInstanceEnabled(false);
          existingInstance.setContainerState(ContainerState.TEARDOWN);
          accessor.setProperty(keyBuilder.instanceConfig(participantId.toString()),
              existingInstance);
          // stop the container
          ContainerId containerId = ContainerId.from(containerIdStr);
          provisioner.stopContainer(containerId);
          existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
          existingInstance.setContainerState(ContainerState.HALTED);
          accessor.setProperty(keyBuilder.instanceConfig(participantId.toString()),
              existingInstance);
        }
      }
    }
  }
}
