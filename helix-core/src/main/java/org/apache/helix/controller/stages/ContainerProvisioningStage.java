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
import org.apache.helix.controller.provisioner.ContainerProvider;
import org.apache.helix.controller.provisioner.ContainerSpec;
import org.apache.helix.controller.provisioner.ContainerState;
import org.apache.helix.controller.provisioner.Provisioner;
import org.apache.helix.controller.provisioner.ProvisionerConfig;
import org.apache.helix.controller.provisioner.ProvisionerRef;
import org.apache.helix.controller.provisioner.TargetProvider;
import org.apache.helix.controller.provisioner.TargetProviderResponse;
import org.apache.helix.model.InstanceConfig;
import org.apache.log4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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
  Map<ResourceId, TargetProvider> _targetProviderMap = new HashMap<ResourceId, TargetProvider>();
  Map<ResourceId, ContainerProvider> _containerProviderMap =
      new HashMap<ResourceId, ContainerProvider>();

  @Override
  public void process(ClusterEvent event) throws Exception {
    final HelixManager helixManager = event.getAttribute("helixmanager");
    final Map<ResourceId, ResourceConfig> resourceMap =
        event.getAttribute(AttributeName.RESOURCES.toString());
    final HelixAdmin helixAdmin = helixManager.getClusterManagmentTool();
    final HelixDataAccessor accessor = helixManager.getHelixDataAccessor();
    final PropertyKey.Builder keyBuilder = accessor.keyBuilder();
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
            provisioner.init(helixManager, resourceConfig);
            _containerProviderMap.put(resourceId, provisioner.getContainerProvider());
            _targetProviderMap.put(resourceId, provisioner.getTargetProvider());
            _provisionerMap.put(resourceId, provisioner);
          } else {
            LOG.error("Resource " + resourceId + " does not have a valid provisioner class!");
            break;
          }
        }
        TargetProvider targetProvider = _targetProviderMap.get(resourceId);
        ContainerProvider containerProvider = _containerProviderMap.get(resourceId);
        final Cluster cluster = event.getAttribute("ClusterDataCache");
        final Collection<Participant> participants = cluster.getParticipantMap().values();

        // TODO: if a process died, we need to mark it as stopped or something

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
        final TargetProviderResponse response =
            targetProvider.evaluateExistingContainers(cluster, resourceId, participants);

        // allocate new containers
        for (final ContainerSpec spec : response.getContainersToAcquire()) {
          // random participant id
          final ParticipantId participantId = ParticipantId.from(UUID.randomUUID().toString());
          // create a new Participant, attach the container spec
          InstanceConfig instanceConfig = new InstanceConfig(participantId);
          instanceConfig.setContainerSpec(spec);
          // create a helix_participant in ACQUIRING state
          instanceConfig.setContainerState(ContainerState.ACQUIRING);
          // create the helix participant and add it to cluster
          helixAdmin.addInstance(cluster.getId().toString(), instanceConfig);

          ListenableFuture<ContainerId> future = containerProvider.allocateContainer(spec);
          FutureCallback<ContainerId> callback = new FutureCallback<ContainerId>() {
            @Override
            public void onSuccess(ContainerId containerId) {
              InstanceConfig existingInstance =
                  helixAdmin
                      .getInstanceConfig(cluster.getId().toString(), participantId.toString());
              existingInstance.setContainerId(containerId);
              existingInstance.setContainerState(ContainerState.ACQUIRED);
              accessor.updateProperty(keyBuilder.instanceConfig(participantId.toString()),
                  existingInstance);
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Could not allocate a container for participant " + participantId, t);
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participantId,
                  ContainerState.FAILED);
            }
          };
          safeAddCallback(future, callback);
        }

        // start new containers
        for (final Participant participant : response.getContainersToStart()) {
          final InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participant.getId()
                  .toString());
          final ContainerId containerId = existingInstance.getContainerId();
          existingInstance.setContainerId(containerId);
          existingInstance.setContainerState(ContainerState.CONNECTING);
          accessor.updateProperty(keyBuilder.instanceConfig(participant.getId().toString()),
              existingInstance);
          // create the helix participant and add it to cluster
          ListenableFuture<Boolean> future = containerProvider.startContainer(containerId, participant);
          FutureCallback<Boolean> callback = new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participant.getId(),
                  ContainerState.ACTIVE);
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Could not start container" + containerId + "for participant "
                  + participant.getId(), t);
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participant.getId(),
                  ContainerState.FAILED);
            }
          };
          safeAddCallback(future, callback);
        }

        // release containers
        for (final Participant participant : response.getContainersToRelease()) {
          // mark it as finalizing
          final InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participant.getId()
                  .toString());
          final ContainerId containerId = existingInstance.getContainerId();
          existingInstance.setContainerState(ContainerState.FINALIZING);
          accessor.updateProperty(keyBuilder.instanceConfig(participant.getId().toString()),
              existingInstance);
          // remove the participant
          ListenableFuture<Boolean> future = containerProvider.deallocateContainer(containerId);
          FutureCallback<Boolean> callback = new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
              InstanceConfig existingInstance =
                  helixAdmin.getInstanceConfig(cluster.getId().toString(), participant.getId()
                      .toString());
              helixAdmin.dropInstance(cluster.getId().toString(), existingInstance);
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Could not deallocate container" + containerId + "for participant "
                  + participant.getId(), t);
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participant.getId(),
                  ContainerState.FAILED);
            }
          };
          safeAddCallback(future, callback);
        }

        // stop but don't remove
        for (final Participant participant : response.getContainersToStop()) {
          // disable the node first
          final InstanceConfig existingInstance =
              helixAdmin.getInstanceConfig(cluster.getId().toString(), participant.getId()
                  .toString());
          final ContainerId containerId = existingInstance.getContainerId();
          existingInstance.setInstanceEnabled(false);
          existingInstance.setContainerState(ContainerState.TEARDOWN);
          accessor.updateProperty(keyBuilder.instanceConfig(participant.getId().toString()),
              existingInstance);
          // stop the container
          ListenableFuture<Boolean> future = containerProvider.stopContainer(containerId);
          FutureCallback<Boolean> callback = new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participant.getId(),
                  ContainerState.HALTED);
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error(
                  "Could not stop container" + containerId + "for participant "
                      + participant.getId(), t);
              updateContainerState(helixAdmin, accessor, keyBuilder, cluster, participant.getId(),
                  ContainerState.FAILED);
            }
          };
          safeAddCallback(future, callback);
        }
      }
    }
  }

  /**
   * Update a participant with a new container state
   * @param helixAdmin
   * @param accessor
   * @param keyBuilder
   * @param cluster
   * @param participantId
   * @param state
   */
  private void updateContainerState(HelixAdmin helixAdmin, HelixDataAccessor accessor,
      PropertyKey.Builder keyBuilder, Cluster cluster, ParticipantId participantId,
      ContainerState state) {
    InstanceConfig existingInstance =
        helixAdmin.getInstanceConfig(cluster.getId().toString(), participantId.toString());
    existingInstance.setContainerState(state);
    accessor.updateProperty(keyBuilder.instanceConfig(participantId.toString()), existingInstance);
  }

  /**
   * Add a callback, failing if the add fails
   * @param future the future to listen on
   * @param callback the callback to invoke
   */
  private <T> void safeAddCallback(ListenableFuture<T> future, FutureCallback<T> callback) {
    try {
      Futures.addCallback(future, callback);
    } catch (Throwable t) {
      callback.onFailure(t);
    }
  }
}
