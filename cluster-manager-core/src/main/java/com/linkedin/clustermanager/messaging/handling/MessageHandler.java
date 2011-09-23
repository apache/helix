package com.linkedin.clustermanager.messaging.handling;

import com.linkedin.clustermanager.NotificationContext;
import com.linkedin.clustermanager.model.Message;

public interface MessageHandler
{
  public void handleMessage(Message message, NotificationContext context)  throws InterruptedException;
}
