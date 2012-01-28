package com.linkedin.helix.messaging.handling;

import com.linkedin.helix.NotificationContext;
import com.linkedin.helix.model.Message;

public interface MessageHandlerFactory
{
  public MessageHandler createHandler(Message message, NotificationContext context);
  
  public String getMessageType();
  
  public void reset();
}
