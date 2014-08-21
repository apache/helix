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

import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModelInfo;
import org.apache.helix.participant.statemachine.StateModelParser;
import org.apache.helix.participant.statemachine.StateTransitionError;
import org.apache.helix.participant.statemachine.Transition;
import org.apache.log4j.Logger;

@StateModelInfo(initialState = "OFFLINE", states = {
    "LEADER", "STANDBY"
})
public class MultiClusterControllerTransitionHandler extends TransitionHandler {
  private static Logger logger = Logger.getLogger(MultiClusterControllerTransitionHandler.class);
  private HelixManager _controller = null;
  private final String _zkAddr;

  public MultiClusterControllerTransitionHandler(String zkAddr) {
    StateModelParser parser = new StateModelParser();
    _currentState = parser.getInitialState(MultiClusterControllerTransitionHandler.class);
    _zkAddr = zkAddr;
  }

  @Transition(to = "STANDBY", from = "OFFLINE")
  public void onBecomeStandbyFromOffline(Message message, NotificationContext context) {
    logger.info("Becoming standby from offline");
  }

  @Transition(to = "LEADER", from = "STANDBY")
  public void onBecomeLeaderFromStandby(Message message, NotificationContext context)
      throws Exception {
    String clusterName = message.getPartitionId().stringify();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becomes leader from standby for " + clusterName);
    // System.out.println(controllerName + " becomes leader from standby for " + clusterName);

    if (_controller == null) {
      _controller =
          HelixManagerFactory.getZKHelixManager(clusterName, controllerName,
              InstanceType.CONTROLLER, _zkAddr);
      _controller.connect();
      _controller.startTimerTasks();
    } else {
      logger.error("controller already exists:" + _controller.getInstanceName() + " for "
          + clusterName);
    }

  }

  @Transition(to = "STANDBY", from = "LEADER")
  public void onBecomeStandbyFromLeader(Message message, NotificationContext context) {
    String clusterName = message.getPartitionId().stringify();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming standby from leader for " + clusterName);

    if (_controller != null) {
      _controller.disconnect();
      _controller = null;
    } else {
      logger.error("No controller exists for " + clusterName);
    }
  }

  @Transition(to = "OFFLINE", from = "STANDBY")
  public void onBecomeOfflineFromStandby(Message message, NotificationContext context) {
    String clusterName = message.getPartitionId().stringify();
    String controllerName = message.getTgtName();

    logger.info(controllerName + " becoming offline from standby for cluster:" + clusterName);

  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context) {
    logger.info("Becoming dropped from offline");
  }

  @Transition(to = "OFFLINE", from = "DROPPED")
  public void onBecomeOfflineFromDropped(Message message, NotificationContext context) {
    logger.info("Becoming offline from dropped");
  }

  @Override
  public void rollbackOnError(Message message, NotificationContext context,
      StateTransitionError error) {
    String clusterName = message.getPartitionId().stringify();
    String controllerName = message.getTgtName();

    logger.error(controllerName + " rollbacks on error for " + clusterName);

    if (_controller != null) {
      _controller.disconnect();
      _controller = null;
    }

  }

  @Override
  public void reset() {
    if (_controller != null) {
      // System.out.println("disconnect " + _controller.getInstanceName()
      // + "(" + _controller.getInstanceType()
      // + ") from " + _controller.getClusterName());
      _controller.disconnect();
      _controller = null;
    }

  }
}
