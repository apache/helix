package org.apache.helix.participant.statemachine;

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

import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.ZNRecord;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.helix.messaging.handling.HelixTaskExecutor;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class ScheduledTaskStateModel extends TransitionHandler {
  static final String DEFAULT_INITIAL_STATE = "OFFLINE";
  Logger logger = Logger.getLogger(ScheduledTaskStateModel.class);

  // TODO Get default state from implementation or from state model annotation
  // StateModel with initial state other than OFFLINE should override this field
  protected String _currentState = DEFAULT_INITIAL_STATE;
  final ScheduledTaskStateModelFactory _factory;
  final ResourceId _resource;
  final PartitionId _partition;

  final HelixTaskExecutor _executor;

  public ScheduledTaskStateModel(ScheduledTaskStateModelFactory factory,
      HelixTaskExecutor executor, ResourceId resource, PartitionId partition) {
    _factory = factory;
    _resource = resource;
    _partition = partition;
    _executor = executor;
  }

  @Transition(to = "COMPLETED", from = "OFFLINE")
  public void onBecomeCompletedFromOffline(Message message, NotificationContext context)
      throws InterruptedException {
    logger.info(_partition + " onBecomeCompletedFromOffline");
    // System.err.println("\t\t" + _partitionName + " onBecomeCompletedFromOffline");

    // Construct the inner task message from the mapfields of scheduledTaskQueue resource group
    Map<String, String> messageInfo =
        message.getRecord().getMapField(Message.Attributes.INNER_MESSAGE.toString());
    ZNRecord record = new ZNRecord(_partition.stringify());
    record.getSimpleFields().putAll(messageInfo);
    Message taskMessage = new Message(record);
    if (logger.isDebugEnabled()) {
      logger.debug(taskMessage.getRecord().getSimpleFields());
    }
    MessageHandler handler =
        _executor.createMessageHandler(taskMessage, new NotificationContext(null));
    if (handler == null) {
      throw new HelixException("Task message " + taskMessage.getMsgType()
          + " handler not found, task id " + _partition);
    }
    // Invoke the internal handler to complete the task
    handler.handleMessage();
    logger.info(_partition + " onBecomeCompletedFromOffline completed");
  }

  @Transition(to = "OFFLINE", from = "COMPLETED")
  public void onBecomeOfflineFromCompleted(Message message, NotificationContext context) {
    logger.info(_partition + " onBecomeOfflineFromCompleted");
  }

  @Transition(to = "DROPPED", from = "COMPLETED")
  public void onBecomeDroppedFromCompleted(Message message, NotificationContext context) {
    logger.info(_partition + " onBecomeDroppedFromCompleted");
    removeFromStatemodelFactory();
  }

  @Transition(to = "DROPPED", from = "OFFLINE")
  public void onBecomeDroppedFromOffline(Message message, NotificationContext context)
      throws InterruptedException {
    logger.info(_partition + " onBecomeDroppedFromScheduled");
    removeFromStatemodelFactory();
  }

  @Transition(to = "OFFLINE", from = "ERROR")
  public void onBecomeOfflineFromError(Message message, NotificationContext context)
      throws InterruptedException {
    logger.info(_partition + " onBecomeOfflineFromError");
  }

  @Override
  public void reset() {
    logger.info(_partition + " ScheduledTask reset");
    removeFromStatemodelFactory();
  }

  // We need this to prevent state model leak
  private void removeFromStatemodelFactory() {
    if (_factory.getTransitionHandler(_resource, _partition) != null) {
      _factory.removeTransitionHandler(_resource, _partition);
    } else {
      logger.warn(_partition + " not found in ScheduledTaskStateModelFactory");
    }
  }
}
