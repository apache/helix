package com.linkedin.clustermanager.messaging.handling;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public interface MessageHandlerFactory
{
  public MessageHandler createHandler(Message message, NotificationContext context);
  
  // TODO: add String getMessageType()
}
