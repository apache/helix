package org.apache.helix.messaging.handling;

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

import java.util.TimerTask;

import org.apache.helix.model.Message;
import org.apache.log4j.Logger;

public class MessageTimeoutTask extends TimerTask {
  private static Logger LOG = Logger.getLogger(MessageTimeoutTask.class);

  final HelixTaskExecutor _executor;
  final MessageTask _task;

  public MessageTimeoutTask(HelixTaskExecutor executor, MessageTask task) {
    _executor = executor;
    _task = task;
  }

  @Override
  public void run() {
    Message message = _task.getMessage();
    // NotificationContext context = _task.getNotificationContext();
    // System.out.println("msg: " + message.getMsgId() + " timeouot.");
    LOG.warn("Message time out, canceling. id:" + message.getMessageId() + " timeout : "
        + message.getExecutionTimeout());
    _task.onTimeout();
    _executor.cancelTask(_task);
  }

}
