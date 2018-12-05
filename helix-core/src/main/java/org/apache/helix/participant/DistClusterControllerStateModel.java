package org.apache.helix.participant;

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

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.controller.pipeline.Pipeline;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@StateModelInfo(initialState = "OFFLINE", states = {
    "LEADER", "STANDBY"
})
public class DistClusterControllerStateModel extends AbstractHelixLeaderStandbyStateModel {
  private static Logger logger = LoggerFactory.getLogger(DistClusterControllerStateModel.class);
  protected HelixManager _controller = null;
  private final Set<Pipeline.Type> _enabledPipelineTypes;

  public DistClusterControllerStateModel(String zkAddr) {
    this(zkAddr, Sets.newHashSet(Pipeline.Type.DEFAULT, Pipeline.Type.TASK));
  }

  public DistClusterControllerStateModel(String zkAddr,
      Set<Pipeline.Type> enabledPipelineTypes) {
    super(zkAddr);
    _enabledPipelineTypes = enabledPipelineTypes;
  }

  @Override
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logStateTransition("OFFLINE", "STANDBY", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = message.getPartitionName();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming leader from standby for " + clusterName);

    if (_controller == null) {
      _controller =
          HelixManagerFactory.getZKHelixManager(clusterName, controllerName,
              InstanceType.CONTROLLER, _zkAddr);
      _controller.setEnabledControlPipelineTypes(_enabledPipelineTypes);
      _controller.connect();
      _controller.startTimerTasks();
      logStateTransition("STANDBY", "LEADER", clusterName, controllerName);
    } else {
      logger.error("controller already exists:" + _controller.getInstanceName() + " for "
          + clusterName);
    }

  }

  @Override
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = message.getPartitionName();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming standby from leader for " + clusterName);

    if (_controller != null) {
      reset();
      logStateTransition("LEADER", "STANDBY", clusterName, controllerName);
    } else {
      logger.error("No controller exists for " + clusterName);
    }
  }

  @Override
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    logStateTransition("STANDBY", "OFFLINE", message.getPartitionName(), message.getTgtName());
  }

  @Override
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    reset();
    logStateTransition("OFFLINE", "DROPPED", message == null ? "" : message.getPartitionName(),
        message == null ? "" : message.getTgtName());
  }

  @Override
  public String getStateModeInstanceDescription(String partitionName, String instanceName) {
    return String.format("Controller for cluster %s on instance %s", partitionName, instanceName);
  }

  @Override
  public void reset() {
    if (_controller != null) {
      _controller.disconnect();
      _controller = null;
    }

  }
}
