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

import java.util.Date;

import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.MessageId;
import org.apache.helix.manager.zk.DefaultControllerMessageHandlerFactory.DefaultControllerMessageHandler;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestDefaultControllerMsgHandlerFactory {
  private static Logger LOG = Logger.getLogger(TestDefaultControllerMsgHandlerFactory.class);

  @Test()
  public void testDefaultControllerMsgHandlerFactory() {
    System.out.println("START TestDefaultControllerMsgHandlerFactory at "
        + new Date(System.currentTimeMillis()));

    DefaultControllerMessageHandlerFactory facotry = new DefaultControllerMessageHandlerFactory();

    Message message = new Message(MessageType.NO_OP, MessageId.from("0"));
    NotificationContext context = new NotificationContext(null);

    boolean exceptionCaught = false;
    try {
      facotry.createHandler(message, context);
    } catch (HelixException e) {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, MessageId.from("1"));
    exceptionCaught = false;
    try {
      facotry.createHandler(message, context);
    } catch (HelixException e) {
      exceptionCaught = true;
    }
    AssertJUnit.assertFalse(exceptionCaught);

    message = new Message(MessageType.NO_OP, MessageId.from("3"));
    DefaultControllerMessageHandler defaultHandler =
        new DefaultControllerMessageHandler(message, context);
    try {
      defaultHandler.handleMessage();
    } catch (HelixException e) {
      exceptionCaught = true;
    } catch (InterruptedException e) {
      LOG.error("Interrupted handling message", e);
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, MessageId.from("4"));
    defaultHandler = new DefaultControllerMessageHandler(message, context);
    exceptionCaught = false;
    try {
      defaultHandler.handleMessage();
    } catch (HelixException e) {
      exceptionCaught = true;
    } catch (InterruptedException e) {
      LOG.error("Interrupted handling message", e);
    }
    AssertJUnit.assertFalse(exceptionCaught);
    System.out.println("END TestDefaultControllerMsgHandlerFactory at "
        + new Date(System.currentTimeMillis()));
  }

}
