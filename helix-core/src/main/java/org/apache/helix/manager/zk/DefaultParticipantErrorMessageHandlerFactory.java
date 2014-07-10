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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.util.Arrays;

import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.NotificationContext;
import org.apache.helix.messaging.handling.HelixTaskResult;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

/**
 * DefaultParticipantErrorMessageHandlerFactory works on controller side.
 * When the participant detects a critical error, it will send the PARTICIPANT_ERROR_REPORT
 * Message to the controller, specifying whether it want to disable the instance or
 * disable the partition. The controller have a chance to do whatever make sense at that point,
 * and then disable the corresponding partition or the instance. More configs per resource will
 * be added to customize the controller behavior.
 */
public class DefaultParticipantErrorMessageHandlerFactory implements MessageHandlerFactory {
  public enum ActionOnError {
    DISABLE_PARTITION,
    DISABLE_RESOURCE,
    DISABLE_INSTANCE
  }

  public static final String ACTIONKEY = "ActionOnError";

  private static Logger _logger = Logger
      .getLogger(DefaultParticipantErrorMessageHandlerFactory.class);
  final HelixManager _manager;

  public DefaultParticipantErrorMessageHandlerFactory(HelixManager manager) {
    _manager = manager;
  }

  public static class DefaultParticipantErrorMessageHandler extends MessageHandler {
    final HelixManager _manager;

    public DefaultParticipantErrorMessageHandler(Message message, NotificationContext context,
        HelixManager manager) {
      super(message, context);
      _manager = manager;
    }

    @Override
    public HelixTaskResult handleMessage() throws InterruptedException {
      HelixTaskResult result = new HelixTaskResult();
      result.setSuccess(true);
      // TODO : consider unify this with StatsAggregationStage.executeAlertActions()
      try {
        ActionOnError actionOnError =
            ActionOnError.valueOf(_message.getRecord().getSimpleField(ACTIONKEY));

        if (actionOnError == ActionOnError.DISABLE_INSTANCE) {
          _manager.getClusterManagmentTool().enableInstance(_manager.getClusterName(),
              _message.getMsgSrc(), false);
          _logger.info("Instance " + _message.getMsgSrc() + " disabled");
        } else if (actionOnError == ActionOnError.DISABLE_PARTITION) {
          _manager.getClusterManagmentTool().enablePartition(false, _manager.getClusterName(),
              _message.getMsgSrc(), _message.getResourceId().stringify(),
              Arrays.asList(_message.getPartitionId().stringify()));
          _logger.info("partition " + _message.getPartitionId() + " disabled");
        } else if (actionOnError == ActionOnError.DISABLE_RESOURCE) {
          // NOT IMPLEMENTED, or we can disable all partitions
          // _manager.getClusterManagmentTool().en(_manager.getClusterName(),
          // _manager.getInstanceName(),
          // _message.getResourceName(), _message.getPartitionName(), false);
          _logger.info("resource " + _message.getResourceId() + " disabled");
        }
      } catch (Exception e) {
        _logger.error("", e);
        result.setSuccess(false);
        result.setException(e);
      }
      return result;
    }

    @Override
    public void onError(Exception e, ErrorCode code, ErrorType type) {
      _logger.error("Message handling pipeline get an exception. MsgId:" + _message.getMessageId(),
          e);
    }

  }

  @Override
  public MessageHandler createHandler(Message message, NotificationContext context) {
    String type = message.getMsgType();

    if (!type.equals(getMessageType())) {
      throw new HelixException("Unexpected msg type for message " + message.getMessageId()
          + " type:" + message.getMsgType());
    }

    return new DefaultParticipantErrorMessageHandler(message, context, _manager);
  }

  @Override
  public String getMessageType() {
    return Message.MessageType.PARTICIPANT_ERROR_REPORT.toString();
  }

  @Override
  public void reset() {

  }

}
