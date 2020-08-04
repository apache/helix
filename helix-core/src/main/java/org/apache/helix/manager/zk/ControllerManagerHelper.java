package org.apache.helix.manager.zk;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.List;

import org.apache.helix.HelixManager;
import org.apache.helix.HelixTimerTask;
import org.apache.helix.PropertyKey;
import org.apache.helix.controller.GenericHelixController;
import org.apache.helix.messaging.DefaultMessagingService;
import org.apache.helix.messaging.handling.MultiTypeMessageHandlerFactory;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * helper class for controller manager
 */
public class ControllerManagerHelper {
  private static Logger LOG = LoggerFactory.getLogger(ControllerManagerHelper.class);

  final HelixManager _manager;
  final DefaultMessagingService _messagingService;
  final List<HelixTimerTask> _controllerTimerTasks;

  public ControllerManagerHelper(HelixManager manager, List<HelixTimerTask> controllerTimerTasks) {
    _manager = manager;
    _messagingService = (DefaultMessagingService) manager.getMessagingService();
    _controllerTimerTasks = controllerTimerTasks;
  }

  public void addListenersToController(GenericHelixController controller) {
    try {
      /**
       * setup controller message listener and register message handlers
       */
      _manager.addControllerMessageListener(_messagingService.getExecutor());
      MultiTypeMessageHandlerFactory defaultControllerMsgHandlerFactory =
          new DefaultControllerMessageHandlerFactory();
      for (String type : defaultControllerMsgHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultControllerMsgHandlerFactory);
      }

      MultiTypeMessageHandlerFactory defaultSchedulerMsgHandlerFactory =
          new DefaultSchedulerMessageHandlerFactory(_manager);
      for (String type : defaultSchedulerMsgHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultSchedulerMsgHandlerFactory);
      }

      MultiTypeMessageHandlerFactory defaultParticipantErrorMessageHandlerFactory =
          new DefaultParticipantErrorMessageHandlerFactory(_manager);

      for (String type : defaultParticipantErrorMessageHandlerFactory.getMessageTypes()) {
        _messagingService.getExecutor()
            .registerMessageHandlerFactory(type, defaultParticipantErrorMessageHandlerFactory);
      }

      /**
       * setup generic-controller
       */
      _manager.addControllerListener(controller);
      _manager.addInstanceConfigChangeListener(controller);
      _manager.addResourceConfigChangeListener(controller);
      _manager.addClusterfigChangeListener(controller);
      _manager.addCustomizedStateConfigChangeListener(controller);
      _manager.addLiveInstanceChangeListener(controller);
      _manager.addIdealStateChangeListener(controller);
    } catch (ZkInterruptedException e) {
      LOG.warn("zk connection is interrupted during HelixManagerMain.addListenersToController(). "
          + e);
    } catch (Exception e) {
      LOG.error("Error when creating HelixManagerContollerMonitor", e);
    }
  }

  public void removeListenersFromController(GenericHelixController controller) {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(_manager.getClusterName());
    /**
     * reset generic-controller
     */
    _manager.removeListener(keyBuilder.idealStates(), controller);
    _manager.removeListener(keyBuilder.liveInstances(), controller);
    _manager.removeListener(keyBuilder.customizedStateConfig(), controller);
    _manager.removeListener(keyBuilder.clusterConfig(), controller);
    _manager.removeListener(keyBuilder.resourceConfigs(), controller);
    _manager.removeListener(keyBuilder.instanceConfigs(), controller);
    _manager.removeListener(keyBuilder.controller(), controller);

    /**
     * reset controller message listener and unregister all message handlers
     */
    _manager.removeListener(keyBuilder.controllerMessages(), _messagingService.getExecutor());
  }

  public void startControllerTimerTasks() {
    for (HelixTimerTask task : _controllerTimerTasks) {
      task.start();
    }
  }

  public void stopControllerTimerTasks() {
    for (HelixTimerTask task : _controllerTimerTasks) {
      task.stop();
    }
  }

}
