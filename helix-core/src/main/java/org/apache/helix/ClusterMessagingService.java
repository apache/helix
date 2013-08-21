package org.apache.helix;

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
import java.util.Map;

import org.apache.helix.messaging.AsyncCallback;
import org.apache.helix.messaging.handling.MessageHandlerFactory;
import org.apache.helix.model.Message;

/**
 * Provides the ability to <br>
 * <li>Send message to a specific component in the cluster[ participant, controller,
 * Spectator(probably not needed) ]</li> <li>Broadcast message to all participants</li> <li>Send
 * message to instances that hold a specific resource</li> <li>Asynchronous request response api.
 * Send message with a co-relation id and invoke a method when there is a response. Can support
 * timeout.</li>
 */
public interface ClusterMessagingService {
  /**
   * Send message matching the specifications mentioned in recipientCriteria.
   * @param receipientCriteria criteria to be met, defined as {@link Criteria}
   * @See Criteria
   * @param message
   *          message to be sent. Some attributes of this message will be
   *          changed as required
   * @return the number of messages that were successfully sent.
   */
  int send(Criteria recipientCriteria, Message message);

  /**
   * This will send the message to all instances matching the criteria<br>
   * When there is a reply to the message sent AsyncCallback.onReply will be
   * invoked. Application can specify a timeout on AsyncCallback. After every
   * reply is processed AsyncCallback.isDone will be invoked.<br>
   * This method will return after sending the messages. <br>
   * This is useful when message need to be sent and current thread need not
   * wait for response since processing will be done in another thread.
   * @see #send(Criteria, Message)
   * @param receipientCriteria
   * @param message
   * @param callbackOnReply callback to trigger on completion
   * @param timeOut Time to wait before failing the send
   * @return the number of messages that were successfully sent
   */
  int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply, int timeOut);

  /**
   * @see #send(Criteria, Message, AsyncCallback, int)
   * @param receipientCriteria
   * @param message
   * @param callbackOnReply
   * @param timeOut
   * @param retryCount maximum number of times to retry the send
   * @return the number of messages that were successfully sent
   */
  int send(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
      int timeOut, int retryCount);

  /**
   * This will send the message to all instances matching the criteria<br>
   * When there is a reply to the message sent AsynCallback.onReply will be
   * invoked. Application can specify a timeout on AsyncCallback. After every
   * reply is processed AsyncCallback.isDone will be invoked.<br>
   * This method will return only after the AsyncCallback.isDone() returns true <br>
   * This is useful when message need to be sent and current thread has to wait
   * for response. <br>
   * The current thread can use callbackOnReply instance to store application
   * specific data.
   * @see #send(Criteria, Message, AsyncCallback, int)
   * @param receipientCriteria
   * @param message
   * @param callbackOnReply
   * @param timeOut
   * @param retryCount
   * @return the number of messages that were successfully sent
   */
  int sendAndWait(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
      int timeOut);

  /**
   * @see #send(Criteria, Message, AsyncCallback, int, int)
   * @param receipientCriteria
   * @param message
   * @param callbackOnReply
   * @param timeOut
   * @param retryCount
   * @return the number of messages that were successfully sent
   */
  int sendAndWait(Criteria receipientCriteria, Message message, AsyncCallback callbackOnReply,
      int timeOut, int retryCount);

  /**
   * This will register a message handler factory to create handlers for
   * message. In case client code defines its own message type, it can define a
   * message handler factory to create handlers to process those messages.
   * Messages are processed in a threadpool which is hosted by cluster manager,
   * and cluster manager will call the factory to create handler, and the
   * handler is called in the threadpool.
   * Note that only one message handler factory can be registered with one
   * message type.
   * @param type
   *          The message type that the factory will create handler for
   * @param factory
   *          The per-type message factory
   * @param threadpoolSize
   *          size of the execution threadpool that handles the message
   */
  public void registerMessageHandlerFactory(String type, MessageHandlerFactory factory);

  /**
   * This will generate all messages to be sent given the recipientCriteria and MessageTemplate,
   * the messages are not sent.
   * @param receipientCriteria criteria to be met, defined as {@link Criteria}
   * @param messageTemplate the Message on which to base the messages to send
   * @return messages to be sent, grouped by the type of instance to send the message to
   */
  public Map<InstanceType, List<Message>> generateMessage(final Criteria recipientCriteria,
      final Message messageTemplate);

}
