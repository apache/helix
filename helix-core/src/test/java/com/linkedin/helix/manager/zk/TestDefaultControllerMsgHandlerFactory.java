/**
 * Copyright (C) 2012 LinkedIn Inc <opensource@linkedin.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.helix.manager.zk;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.linkedin.helix.HelixException;
import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.manager.zk.DefaultControllerMessageHandlerFactory;
import com.linkedin.helix.manager.zk.DefaultControllerMessageHandlerFactory.DefaultControllerMessageHandler;
import com.linkedin.helix.messaging.handling.MessageHandler;
import com.linkedin.helix.model.Message;
import com.linkedin.helix.model.Message.MessageType;

public class TestDefaultControllerMsgHandlerFactory
{
  @Test()
  public void testDefaultControllerMsgHandlerFactory()
  {
  	System.out.println("START TestDefaultControllerMsgHandlerFactory at " + new Date(System.currentTimeMillis()));

    DefaultControllerMessageHandlerFactory facotry = new DefaultControllerMessageHandlerFactory();

    Message message = new Message(MessageType.NO_OP, "0");
    NotificationContext context = new NotificationContext(null);

    boolean exceptionCaught = false;
    try
    {
      MessageHandler handler = facotry.createHandler(message, context);
    } catch (HelixException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, "1");
    exceptionCaught = false;
    try
    {
      MessageHandler handler = facotry.createHandler(message, context);
    } catch (HelixException e)
    {
      exceptionCaught = true;
    }
    AssertJUnit.assertFalse(exceptionCaught);

    Map<String, String> resultMap = new HashMap<String, String>();
    message = new Message(MessageType.NO_OP, "3");
    DefaultControllerMessageHandler defaultHandler = new DefaultControllerMessageHandler(message, context);
    try
    {
      defaultHandler.handleMessage();
    } catch (HelixException e)
    {
      exceptionCaught = true;
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    AssertJUnit.assertTrue(exceptionCaught);

    message = new Message(MessageType.CONTROLLER_MSG, "4");
    defaultHandler = new DefaultControllerMessageHandler(message, context);
    exceptionCaught = false;
    try
    {
      defaultHandler.handleMessage();
    } catch (HelixException e)
    {
      exceptionCaught = true;
    }
    catch (InterruptedException e)
    {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    AssertJUnit.assertFalse(exceptionCaught);
    System.out.println("END TestDefaultControllerMsgHandlerFactory at " + new Date(System.currentTimeMillis()));
  }

}
