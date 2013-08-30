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
import java.util.HashMap;
import java.util.Map;

import org.apache.helix.HelixException;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.Id;
import org.apache.helix.manager.zk.DefaultControllerMessageHandlerFactory.DefaultControllerMessageHandler;
import org.apache.helix.messaging.handling.MessageHandler;
import org.apache.helix.model.Message;
import org.apache.helix.model.Message.MessageType;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

public class TestDefaultControllerMsgHandlerFactory {
  @Test()
  public void testDefaultControllerMsgHandlerFactory() {
    System.out.println("START TestDefaultControllerMsgHandlerFactory at "
        + new Date(System.currentTimeMillis()));

    DefaultControllerMessageHandlerFactory facotry = new DefaultControllerMessageHandlerFactory();

    Message message = new Message(MessageType.NO_OP, Id.message("0"));
    NotificationContext context = new NotificationContext(null);

    boolean exceptionCaught = false;
    try {
      MessageHandler handler = facotry.createHandler(message, context);
    } catch (HelixException e) {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, Id.message("1"));
    exceptionCaught = false;
    try {
      MessageHandler handler = facotry.createHandler(message, context);
    } catch (HelixException e) {
      exceptionCaught = true;
    }
    AssertJUnit.assertFalse(exceptionCaught);

    Map<String, String> resultMap = new HashMap<String, String>();
    message = new Message(MessageType.NO_OP, Id.message("3"));
    DefaultControllerMessageHandler defaultHandler =
        new DefaultControllerMessageHandler(message, context);
    try {
      defaultHandler.handleMessage();
    } catch (HelixException e) {
      exceptionCaught = true;
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, Id.message("4"));
    defaultHandler = new DefaultControllerMessageHandler(message, context);
    exceptionCaught = false;
    try {
      defaultHandler.handleMessage();
    } catch (HelixException e) {
      exceptionCaught = true;
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(exceptionCaught);
    System.out.println("END TestDefaultControllerMsgHandlerFactory at "
        + new Date(System.currentTimeMillis()));
  }

}
