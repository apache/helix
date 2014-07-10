package org.apache.helix.provisioning.yarn;

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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.helix.provisioning.ContainerAskResponse;
import org.apache.helix.provisioning.ContainerReleaseResponse;
import org.apache.helix.provisioning.ContainerStopResponse;

import com.google.common.util.concurrent.SettableFuture;

class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
  private static final Log LOG = LogFactory.getLog(RMCallbackHandler.class);
  long startTime;
  /**
   *
   */
  private final GenericApplicationMaster _genericApplicationMaster;

  /**
   * @param genericApplicationMaster
   */
  RMCallbackHandler(GenericApplicationMaster genericApplicationMaster) {
    _genericApplicationMaster = genericApplicationMaster;
    startTime = System.currentTimeMillis();
  }

  @Override
  public void onContainersCompleted(List<ContainerStatus> completedContainers) {
    LOG.info("Got response from RM for container ask, completedCnt=" + completedContainers.size());
    for (ContainerStatus containerStatus : completedContainers) {
      GenericApplicationMaster.LOG.info("Got container status for containerID="
          + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
          + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
          + containerStatus.getDiagnostics());

      // non complete containers should not be here
      assert (containerStatus.getState() == ContainerState.COMPLETE);
      synchronized (_genericApplicationMaster.allocatedContainerSet) {
        _genericApplicationMaster.allocatedContainerSet.remove(containerStatus.getContainerId());
        SettableFuture<ContainerStopResponse> stopResponseFuture =
            _genericApplicationMaster.containerStopMap.remove(containerStatus.getContainerId());
        if (stopResponseFuture != null) {
          ContainerStopResponse value = new ContainerStopResponse();
          stopResponseFuture.set(value);
        } else {
          SettableFuture<ContainerReleaseResponse> releaseResponseFuture =
              _genericApplicationMaster.containerReleaseMap
                  .remove(containerStatus.getContainerId());
          if (releaseResponseFuture != null) {
            ContainerReleaseResponse value = new ContainerReleaseResponse();
            releaseResponseFuture.set(value);
          }
        }
      }
      // increment counters for completed/failed containers
      int exitStatus = containerStatus.getExitStatus();
      if (0 != exitStatus) {
        // container failed
        if (ContainerExitStatus.ABORTED != exitStatus) {

        } else {
          // container was killed by framework, possibly preempted
          // we should re-try as the container was lost for some reason

          // we do not need to release the container as it would be done
          // by the RM
        }
      } else {
        // nothing to do
        // container completed successfully
        GenericApplicationMaster.LOG.info("Container completed successfully." + ", containerId="
            + containerStatus.getContainerId());
      }
    }
  }

  @Override
  public void onContainersAllocated(List<Container> allocatedContainers) {
    GenericApplicationMaster.LOG.info("Got response from RM for container ask, allocatedCnt="
        + allocatedContainers.size());
    for (Container allocatedContainer : allocatedContainers) {
      GenericApplicationMaster.LOG.info("Allocated new container." + ", containerId="
          + allocatedContainer.getId() + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory"
          + allocatedContainer.getResource().getMemory());
      for (ContainerRequest containerRequest : _genericApplicationMaster.containerRequestMap
          .keySet()) {
        if (containerRequest.getCapability().getMemory() == allocatedContainer.getResource()
            .getMemory()) {
          SettableFuture<ContainerAskResponse> future =
              _genericApplicationMaster.containerRequestMap.remove(containerRequest);
          ContainerAskResponse response = new ContainerAskResponse();
          response.setContainer(allocatedContainer);
          _genericApplicationMaster.allocatedContainerSet.add(allocatedContainer.getId());
          future.set(response);
          break;
        }
      }
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
    return (System.currentTimeMillis() - startTime) % Integer.MAX_VALUE;
  }

  @Override
  public void onError(Throwable e) {
    _genericApplicationMaster.amRMClient.stop();
  }
}
